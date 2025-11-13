//go:build !windows

package netboot

import "errors"

func tryWinINET(string) (*profile, error) {
	return nil, errors.New("wininet not supported on non-windows")
}

func tryWinHTTP(string) (*profile, error) {
	return nil, errors.New("winhttp not supported on non-windows")
}
