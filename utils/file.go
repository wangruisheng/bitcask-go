package utils

import (
	"io/fs"
	"path/filepath"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

//// AvailableDiskSize 磁盘剩余的可用空间大小
//func AvailableDiskSize() (uint64, error) {
//	wd, err := syscall.Getwd()
//	if err != nil {
//		return 0, err
//	}
//	fs.StatFS()
//	var stat syscall.Statfs_t
//	if err = syscall.Statfs(wd, &stat); err != nil {
//		return 0, err
//	}
//	return stat.Bavail * uint64(stat.Bsize), nil
//}
//
//func AvailableDiskSize2() (uint64, error) {
//	wd, err := syscall.Getwd()
//	windows.Getwd()
//	if err != nil {
//		return 0, err
//	}
//	var stat syscall.Statfs_t
//	if err = syscall.Statfs(wd, &stat); err != nil {
//		return 0, err
//	}
//	return stat.Bavail * uint64(stat.Bsize), nil
//}
