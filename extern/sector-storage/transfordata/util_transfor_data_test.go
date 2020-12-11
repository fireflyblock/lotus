package transfordata

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	//files,err:=ioutil.ReadDir(".")
	//if err!=nil{ //	fmt.Printf("%v",err)
	//}
	//for _,file:=range files{
	//	fmt.Printf("name :%s\n",file.Name())
	//}

	//var path = "/home/firefly/storage/storage-10.11-00/f02420"
	var path = "/opt/lotus/storage/storage-20.164-00"
	path = strings.TrimRight(path, fmt.Sprintf("%c", os.PathSeparator))
	fmt.Printf("path = %s\n", path)
	base := filepath.Base(path)
	fmt.Printf("base = %s\n", base)
	fmt.Printf("basePre = %s\n", filepath.Base(strings.TrimRight(path, base)))
}

func TestTarSpecialFiles(t *testing.T) {
	// 目标文件，压缩后的文件
	//var srcPath="./s-t088290-2300"
	var srcPath = "./s-t01002-2"
	var dst = filepath.Base(srcPath) + ".tar.gz"
	var buf bytes.Buffer
	//err := TarSpecialFiles(srcPath, &buf)
	err := Tar(srcPath, &buf)
	if err != nil {
		fmt.Printf("create tar failed: %s", err)
	}

	// write the .tar.gzip
	fileToWrite, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		fmt.Printf("open file failed: %s", err)
	}

	if _, err := io.Copy(fileToWrite, &buf); err != nil {
		fmt.Printf("copy file failed: %s", err)
	}

	err = fileToWrite.Close()
	if err != nil {
		fmt.Printf("close file failed: %s", err)
	}
}

func TestPareseDestFromePath(t *testing.T) {
	//var path1 = "/home/firefly/storage/storage-10.11-00/f02420"
	//var path2 = "/home/firefly/storage/storage-10.11-00"
	var path3 = "/opt/lotus/storage/storage-20.164-00/"

	//ip1,dest1:=PareseDestFromePath(path1)
	//ip2,dest2:=PareseDestFromePath(path2)
	ip3, dest3 := PareseDestFromePath(path3)
	//fmt.Printf("\nip1 = %s, dest1 = %s\n",ip1,dest1)
	//fmt.Printf("\nip2 = %s, dest2 = %s\n",ip2,dest2)
	fmt.Printf("\nip3 = %s, dest3 = %s\n", ip3, dest3)

}
