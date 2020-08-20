package stores

import (
	"archive/zip"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// 通过storagePath来解析NFS挂载路径和NFS源IP
// 要求NFS挂载源格式统一：/mnt/nfs1 和 /mnt/nfs2
// 要求NFS挂载目标命名统一：storage-10.11-00 和 storage-10.11-01
func PareseDestFromePath(storagePath string) (ip, destPath string) {
	spath := filepath.Base(storagePath)
	ip = "172.16."
	destPath = "/mnt/nfs"
	sp := strings.Split(spath, "-")
	if len(sp) != 3 {
		ip = ""
		destPath = ""
		return ip, destPath
	}
	ip = fmt.Sprintf("%s%s", ip, sp[1])
	if sp[2] == "00" {
		destPath = fmt.Sprintf("%s%s", destPath, "1")
	} else if sp[2] == "01" {
		destPath = fmt.Sprintf("%s%s", destPath, "2")
	} else {
		destPath = ""
	}
	return ip, destPath
}

// srcPath 源文件路径
// src 准备压缩的文件或目录
// dstPath
// ip 目标服务器 ip 地址
func SendZipFile(srcPath, src, dstPath, ip string) error {
	// 目标文件，压缩后的文件
	var dst = src + ".zip"

	_ = os.Rename(srcPath+src, "./"+src)
	if err := Zip(dst, src); err != nil {
		log.Errorf("create zip err:", err)
		return err
	}

	r, w := io.Pipe()
	m := multipart.NewWriter(w)

	go func() {
		defer w.Close()
		defer m.Close()
		part, err := m.CreateFormFile("file", dst)
		if err != nil {
			return
			//panic(err)
		}

		file, err := os.Open(dst)
		if err != nil {
			return
			//panic(err)
		}
		defer file.Close()
		if _, err = io.Copy(part, file); err != nil {
			return
			//panic(err)
		}
	}()

	port := os.Getenv("STORAGE_SERVICE_PORT")
	if port == "" {
		port = ":8080"
	}

	url := "http://" + ip + port + "/uploadZip" + "?path=" + dstPath + "&" + "src=" + src
	_, err := http.Post(url, m.FormDataContentType(), r)
	if err != nil {
		log.Errorf("http post request err %s", err.Error())
		return err
	}

	_ = os.Remove(dst)
	err = os.RemoveAll(src)
	if err != nil {
		return err
		//panic(err)
	}
	return nil
}

// srcPath 源文件路径
// src 准备压缩的文件或目录
// dstPath
// ip 目标服务器 ip 地址
func SendFile(srcPath, src, dstPath, ip string) error {
	r, w := io.Pipe()
	m := multipart.NewWriter(w)

	go func() {
		defer w.Close()
		defer m.Close()
		part, err := m.CreateFormFile("file", src)
		if err != nil {
			log.Errorf("CreateFormFile failed, err %s", err.Error())
			return
		}

		file, err := os.Open(srcPath + src)
		if err != nil {
			return
			//panic(err)
		}
		defer file.Close()
		if _, err = io.Copy(part, file); err != nil {
			log.Errorf("io copy failed, err %s", err.Error())
			return
		}
	}()

	port := os.Getenv("STORAGE_SERVICE_PORT")
	if port == "" {
		port = ":8080"
	}

	url := "http://" + ip + port + "/upload" + "?path=" + dstPath + "&" + "src=" + src
	_, err := http.Post(url, m.FormDataContentType(), r)
	if err != nil {
		log.Errorf("http post request err %s", err.Error())
		return err
	}
	err = os.Remove(srcPath + src)
	if err != nil {
		return err
		//panic(err)
	}
	return nil
}

func Zip(dst, src string) (err error) {
	// 创建准备写入的文件
	fw, err := os.Create(dst)
	if fw == nil {
		return nil
	}
	defer fw.Close()
	if err != nil {
		return err
	}

	// 通过 fw 来创建 zip.Write
	zw := zip.NewWriter(fw)
	defer func() {
		// 检测一下是否成功关闭
		if err := zw.Close(); err != nil {
			log.Errorf("", err)
		}
	}()

	// 下面来将文件写入 zw ，因为有可能会有很多个目录及文件，所以递归处理
	return filepath.Walk(src, func(path string, fi os.FileInfo, errBack error) (err error) {
		if errBack != nil {
			return errBack
		}

		// 通过文件信息，创建 zip 的文件信息
		fh, err := zip.FileInfoHeader(fi)
		if err != nil {
			return
		}

		// 替换文件信息中的文件名
		fh.Name = strings.TrimPrefix(path, string(filepath.Separator))

		// 这步开始没有加，会发现解压的时候说它不是个目录
		if fi.IsDir() {
			fh.Name += "/"
		}

		// 写入文件信息，并返回一个 Write 结构
		w, err := zw.CreateHeader(fh)
		if err != nil {
			return
		}

		// 检测，如果不是标准文件就只写入头信息，不写入文件数据到 w
		// 如目录，也没有数据需要写
		if !fh.Mode().IsRegular() {
			return nil
		}

		// 打开要压缩的文件
		fr, err := os.Open(path)
		if fr == nil {
			panic("fr == nil")
		}
		defer fr.Close()
		if err != nil {
			return
		}

		// 将打开的文件 Copy 到 w
		_, err = io.Copy(w, fr)
		if err != nil {
			return
		}

		return nil
	})
}
