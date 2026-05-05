//go:build windows

package syncfs

import (
	"io/fs"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type duplicateExtentsData struct {
	fileHandle       windows.Handle
	sourceFileOffset int64
	targetFileOffset int64
	byteCount        int64
}

func cloneFile(src, dest string, _ fs.FileMode) (bool, error) {
	info, err := os.Stat(src)
	if err != nil {
		return false, err
	}

	source, err := openCloneSource(src)
	if err != nil {
		return false, err
	}
	defer func() { _ = windows.CloseHandle(source) }()

	target, err := openCloneTarget(dest)
	if err != nil {
		return false, err
	}
	defer func() { _ = windows.CloseHandle(target) }()

	if err := windows.Ftruncate(target, info.Size()); err != nil {
		return false, nil
	}

	if info.Size() == 0 {
		return true, nil
	}

	req := duplicateExtentsData{
		fileHandle: source,
		byteCount:  info.Size(),
	}
	var bytesReturned uint32

	err = windows.DeviceIoControl(
		target,
		windows.FSCTL_DUPLICATE_EXTENTS_TO_FILE,
		(*byte)(unsafe.Pointer(&req)),
		uint32(unsafe.Sizeof(req)),
		nil,
		0,
		&bytesReturned,
		nil,
	)
	if err != nil {
		return false, nil
	}

	return true, nil
}

func openCloneSource(path string) (windows.Handle, error) {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}

	return windows.CreateFile(
		ptr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
}

func openCloneTarget(path string) (windows.Handle, error) {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}

	return windows.CreateFile(
		ptr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_DELETE,
		nil,
		windows.CREATE_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
}
