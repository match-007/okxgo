//go:build windows

package netboot

import (
	"errors"
	"net/url"
	"os/exec"
	"strings"

	"golang.org/x/sys/windows/registry"
)

// 读取 WinINET（IE/Edge 使用）代理设置
func tryWinINET(target string) (*profile, error) {
	k, err := registry.OpenKey(registry.CURRENT_USER,
		`Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings`,
		registry.QUERY_VALUE)
	if err != nil {
		return nil, err
	}
	defer k.Close()

	enabled, _, _ := k.GetIntegerValue("ProxyEnable")
	if enabled != 1 {
		// ProxyEnable=0 可能是 PAC 模式；Go 不解析 PAC，这里跳过到 WinHTTP/候选端口
		return nil, errors.New("wininet proxy disabled or PAC")
	}

	server, _, err := k.GetStringValue("ProxyServer")
	if err != nil || strings.TrimSpace(server) == "" {
		return nil, errors.New("wininet no ProxyServer")
	}

	scheme, hp := pickProxyFromKV(server)
	if hp == "" {
		return nil, errors.New("wininet parse ProxyServer failed")
	}
	u, _ := url.Parse(scheme + "://" + hp) // WinINET 一般是 HTTP 代理
	tr := buildTransport(u, nil, nil, false)
	tag := "WinINET " + u.Host
	return probe(target, tr, tag)
}

// 读取 WinHTTP（系统级代理）
func tryWinHTTP(target string) (*profile, error) {
	out, err := exec.Command("netsh", "winhttp", "show", "proxy").CombinedOutput()
	if err != nil {
		return nil, err
	}
	s := strings.ReplaceAll(string(out), "\r\n", "\n")

	// 先判断直连
	if strings.Contains(s, "Direct access (no proxy server)") ||
		strings.Contains(s, "Direct access") ||
		strings.Contains(s, "直接访问") {
		return nil, errors.New("winhttp direct")
	}

	// 英文/中文两种关键字
	lines := strings.Split(s, "\n")
	line := ""
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if strings.Contains(ln, "Proxy Server") || strings.Contains(ln, "代理服务器") {
			line = ln
			break
		}
	}
	if line == "" {
		return nil, errors.New("winhttp no proxy line")
	}

	// 取冒号后的部分
	kv := line
	if pos := strings.Index(line, ":"); pos >= 0 {
		kv = strings.TrimSpace(line[pos+1:])
	}
	scheme, hp := pickProxyFromKV(kv)
	if hp == "" {
		return nil, errors.New("winhttp parse failed")
	}

	u, _ := url.Parse(scheme + "://" + hp)
	tr := buildTransport(u, nil, nil, false)
	tag := "WinHTTP " + u.Host
	return probe(target, tr, tag)
}
