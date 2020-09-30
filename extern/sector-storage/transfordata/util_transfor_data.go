package transfordata

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var log = logging.Logger("transfordata") // nolint
// 通过storagePath来解析NFS挂载路径和NFS源IP
// 要求NFS挂载源格式统一：/mnt/nfs1 和 /mnt/nfs2
// 要求NFS挂载目标命名统一：storage-10.11-00 和 storage-10.11-01
func PareseDestFromePath(storagePath string) (ip, destPath string) {
	spath := filepath.Base(storagePath)
	ip = "172.16."
	destPath = "/mnt/nfs"
	ip, destPath = GetStorageInfo()
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

func ConnectTest(path, ip string) (bool, error) {

	port := os.Getenv("STORAGE_SERVICE_PORT")
	if port == "" {
		port = ":8080"
	}

	url := "http://" + ip + port + "/connect" + "?path=" + path
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("ConnectTest failed, ", err.Error())
		return false, err
	}
	defer response.Body.Close()

	if response.StatusCode == 200 {
		return true, nil
	} else {
		body, err := ioutil.ReadAll(response.Body)
		return false, xerrors.Errorf("ConnectTest ip(%s) error, body : %s ,err: %+v", ip, string(body), err)
	}
}

// srcPath 源文件路径
// src 准备压缩的文件或目录
// dstPath
// ip 目标服务器 ip 地址
func SendZipFile(srcPath, src, dstPath, ip string) error {
	// 目标文件，压缩后的文件
	var dst = src + ".tar.gz"
	var buf bytes.Buffer
	err := Tar(srcPath+src, &buf)
	if err != nil {
		log.Errorf("create tar failed: %s", err)
		return err
	}

	// write the .tar.gzip
	fileToWrite, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		log.Errorf("open file failed: %s", err)
		return err
	}

	if _, err := io.Copy(fileToWrite, &buf); err != nil {
		log.Errorf("copy file failed: %s", err)
		return err
	}

	err = fileToWrite.Close()
	if err != nil {
		log.Errorf("close file failed: %s", err)
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
	res, err := http.Post(url, m.FormDataContentType(), r)
	if err != nil {
		log.Errorf("http post request err %s", err.Error())
		return err
	}

	if res.StatusCode != 200 {
		return xerrors.Errorf("ConnectTest failed")
	}

	err = os.Remove(dst)
	if err != nil {
		log.Errorf("remove dst failed, %s", err.Error())
		return err
	}

	//err = os.RemoveAll(srcPath + src)
	//if err != nil {
	//	log.Errorf("remove src failed, %s", err.Error())
	//	return err
	//}
	return nil
}

// srcPath 源文件路径
// src 准备压缩的文件或目录
// dstPath
// ip 目标服务器 ip 地址
func SendFile(srcPath, src, dstPath, ip string) (int, error) {
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
	resp, err := http.Post(url, m.FormDataContentType(), r)
	if err != nil {
		log.Errorf("http post request err %s", err.Error())
		return -1, err
	}
	if resp.StatusCode != 200 {
		return resp.StatusCode, xerrors.Errorf("ConnectTest failed")
	}

	//err = os.Remove(srcPath + src)
	//if err != nil {
	//	return err
	//	//panic(err)
	//}
	return resp.StatusCode, nil
}

//func ConnectTest(path, ip string) error {
//	port := os.Getenv("STORAGE_SERVICE_PORT")
//	if port == "" {
//		port = ":8080"
//	}
//
//	url := "http://" + ip + port + "/connect" + "?path=" + path
//	response, err := http.Get(url)
//	if err != nil {
//		log.Errorf("ConnectTest failed, %s", err.Error())
//		return xerrors.Errorf("ConnectTest failed, ", err.Error())
//	}
//
//	if response.StatusCode != 200 {
//		return xerrors.Errorf("ConnectTest failed")
//	}
//
//	return nil
//}

func Tar(src string, writers ...io.Writer) error {

	// ensure the src actually exists before trying to tar it
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("Unable to tar files - %v ", err.Error())
	}

	mw := io.MultiWriter(writers...)

	gzw := gzip.NewWriter(mw)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// walk path
	return filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {

		// return on any error
		if err != nil {
			return err
		}

		// return on non-regular files (thanks to [kumo](https://medium.com/@komuw/just-like-you-did-fbdd7df829d3) for this suggested update)
		if !fi.Mode().IsRegular() {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(strings.Replace(file, src, "", -1), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := io.Copy(tw, f); err != nil {
			return err
		}

		// manually close here after each file operation; defering would cause each file close
		// to wait until all operations have completed.
		f.Close()

		return nil
	})
}

type Config struct {
	RecordUrl   string
	RedisUrl    string
	PassWord    string
	StorageIP   string
	StoragePath string
}

func initRequestConfig(filePath string) (*Config, error) {
	var confInfo = new(Config)
	if !isFileExist(filePath) {
		// default value
		confInfo.StoragePath = "/mnt/nfs"
		confInfo.StorageIP = "172.16."
		fn, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		defer fn.Close()
		encoder := json.NewEncoder(fn)
		err = encoder.Encode(confInfo)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	err = decoder.Decode(confInfo)
	if err != nil {
		return nil, err
	}

	return confInfo, nil
}

func GetStorageInfo() (string, string) {
	conf, err := initRequestConfig("conf.json")
	if err != nil {
		log.Errorf("GetStorageInfo Error: %+v", err)
		return "172.16.", "/mnt/nfs"
	}
	return conf.StorageIP, conf.StoragePath
}

func isFileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return true
}
