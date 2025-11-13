package netboot

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	xnetproxy "golang.org/x/net/proxy"
)

type profile struct {
	httpTransport *http.Transport

	httpProxyURL *url.URL         // 若为 HTTP 代理
	socksDialer  xnetproxy.Dialer // 若为 SOCKS5 代理
	resolver     *net.Resolver    // 若启用干净 DNS

	desc string // 日志描述
}

// Init 全局初始化：设置 http.DefaultTransport/Client 和 websocket.DefaultDialer
// 策略：环境代理 -> Windows 系统代理(WinINET/WinHTTP) -> 常见本地代理 -> 直连(必要时切换干净DNS)
func Init() error {
	target := "https://www.okx.com/api/v5/public/time"

	// 1) 环境代理（HTTP(S)_PROXY/ALL_PROXY/NO_PROXY）
	if p, err := tryEnv(target); err == nil {
		apply(p)
		return nil
	}

	// 1.1) Windows: WinINET（IE/Edge 的系统代理）
	if p, err := tryWinINET(target); err == nil {
		apply(p)
		return nil
	}

	// 1.2) Windows: WinHTTP（系统级代理，很多非浏览器程序依赖）
	if p, err := tryWinHTTP(target); err == nil {
		apply(p)
		return nil
	}

	// 2) 常见本地代理（仅成功才采用）
	cand := []string{
		"http://127.0.0.1:7890",
		"http://127.0.0.1:10809",
		"http://127.0.0.1:1080",
		"socks5://127.0.0.1:7891",
		"socks5://127.0.0.1:1080",
	}
	for _, s := range cand {
		if p, err := tryProxy(target, s); err == nil {
			apply(p)
			return nil
		}
	}

	// 3) 直连（若 DNS 可疑则切换干净 DNS）
	needClean, _ := looksPolluted("www.okx.com")
	if p, err := tryDirect(target, needClean); err == nil {
		apply(p)
		return nil
	}

	log.Printf("❌ 网络引导失败：环境/系统代理/本地代理/直连均不可用，请检查防火墙或手工指定代理。")
	return errors.New("network bootstrap failed")
}

// —— 构建与探活 —— //

func tryEnv(target string) (*profile, error) {
	tr := buildTransport(nil, nil, nil, true)
	return probe(target, tr, "EnvProxy")
}

func tryProxy(target, proxyStr string) (*profile, error) {
	u, err := url.Parse(proxyStr)
	if err != nil {
		return nil, err
	}
	tr := buildTransport(u, nil, nil, false)
	tag := "Proxy " + u.Scheme + " " + u.Host
	return probe(target, tr, tag)
}

func tryDirect(target string, cleanDNS bool) (*profile, error) {
	var res *net.Resolver
	if cleanDNS {
		res = publicResolver("1.1.1.1:53")
	}
	tr := buildTransport(nil, res, nil, false)
	tag := "Direct"
	if res != nil {
		tag += " + CleanDNS(1.1.1.1)"
	}
	return probe(target, tr, tag)
}

// 解析 WinINET/WinHTTP 的代理字符串，如：
// "http=127.0.0.1:7890;https=127.0.0.1:7890" 或 "127.0.0.1:7890"
func pickProxyFromKV(s string) (scheme, hostport string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ";")
	var https, httpv, naked string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			k := strings.ToLower(strings.TrimSpace(kv[0]))
			v := strings.TrimSpace(kv[1])
			switch k {
			case "https":
				https = v
			case "http":
				httpv = v
			}
		} else if naked == "" {
			naked = p
		}
	}
	if https != "" {
		return "http", https
	}
	if httpv != "" {
		return "http", httpv
	}
	if naked != "" {
		return "http", naked
	}
	return "", ""
}

func probe(url string, tr *http.Transport, tag string) (*profile, error) {
	c := &http.Client{Timeout: 6 * time.Second, Transport: tr}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("probe http status != 200")
	}

	// 反推 WS 配置
	p := &profile{httpTransport: tr, desc: tag}
	if tr.Proxy != nil {
		if u, _ := tr.Proxy(req); u != nil {
			if u.Scheme == "socks5" {
				if d, err := xnetproxy.FromURL(u, nil); err == nil {
					p.socksDialer = d
					p.desc = "SOCKS5 " + u.Host
				}
			} else {
				p.httpProxyURL = u
				p.desc = "HTTP Proxy " + u.Host
			}
		}
	}
	// 直连 + 干净 DNS（我们在 buildTransport 时通过 resolver 决定 DialContext）
	if p.httpProxyURL == nil && p.socksDialer == nil {
		if tr.DialContext != nil {
			// 与构建时一致：用固定公用 DNS
			p.resolver = publicResolver("1.1.1.1:53")
		}
	}
	return p, nil
}

// —— 应用到全局 —— //

func apply(p *profile) {
	// HTTP: 默认 Client/Transport
	http.DefaultTransport = p.httpTransport
	http.DefaultClient = &http.Client{
		Transport: p.httpTransport,
		Timeout:   30 * time.Second,
	}

	// WS: 默认 Dialer 与 HTTP 保持一致
	d := *websocket.DefaultDialer // 复制
	if p.socksDialer != nil {
		d.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return p.socksDialer.Dial(network, addr)
		}
	} else if p.httpProxyURL != nil {
		d.Proxy = http.ProxyURL(p.httpProxyURL)
	} else if p.resolver != nil {
		d.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, _ := net.SplitHostPort(address)
			ips, err := p.resolver.LookupIPAddr(ctx, host)
			if err != nil || len(ips) == 0 {
				return nil, err
			}
			dialer := &net.Dialer{Timeout: 10 * time.Second}
			for _, ip := range ips {
				if conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port)); err == nil {
					return conn, nil
				}
			}
			return nil, errors.New("ws all IPs failed")
		}
	} else {
		d.Proxy = http.ProxyFromEnvironment
	}
	websocket.DefaultDialer = &d

	log.Printf("🌐 全局网络就绪：%s", p.desc)
}

// —— 传输构建 —— //

// proxyURL==nil 且 useEnv==true：使用环境代理
// proxyURL=http/https：作为 HTTP 代理
// proxyURL=socks5：使用 SOCKS5 拨号
// resolver!=nil：直连时指定干净 DNS（规避 169.254.* / Fake-IP）
func buildTransport(proxyURL *url.URL, resolver *net.Resolver, tlsCfg *tls.Config, useEnv bool) *http.Transport {
	tr := &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}

	if proxyURL != nil {
		if proxyURL.Scheme == "socks5" {
			dialer, err := xnetproxy.FromURL(proxyURL, nil)
			if err == nil {
				tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					return dialer.Dial(network, address)
				}
			}
		} else {
			tr.Proxy = http.ProxyURL(proxyURL)
		}
	} else if useEnv {
		tr.Proxy = http.ProxyFromEnvironment
	}

	if resolver != nil && tr.DialContext == nil {
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, _ := net.SplitHostPort(address)
			ips, err := resolver.LookupIPAddr(ctx, host)
			if err != nil || len(ips) == 0 {
				return nil, err
			}
			d := &net.Dialer{Timeout: 10 * time.Second}
			for _, ip := range ips {
				if conn, err := d.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port)); err == nil {
					return conn, nil
				}
			}
			return nil, errors.New("all IPs failed")
		}
	}
	return tr
}

// —— DNS 工具 —— //

func looksPolluted(host string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return false, err
	}
	if len(addrs) == 0 {
		return true, nil
	}
	for _, a := range addrs {
		ip := a.IP
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
			return true, nil
		}
	}
	return false, nil
}

func publicResolver(addr string) *net.Resolver {
	d := func(ctx context.Context, network, _ string) (net.Conn, error) {
		// 先试 TCP，失败再回退 UDP
		if strings.HasPrefix(network, "tcp") {
			return net.DialTimeout("tcp", addr, 2*time.Second)
		}
		return net.DialTimeout("udp", addr, 2*time.Second)
	}
	return &net.Resolver{PreferGo: true, Dial: d}
}

// 包被导入时自动初始化（你也可以在 main 里手动调用 Init）
func init() {
	if err := Init(); err != nil {
		log.Printf("network bootstrap failed: %v", err)
	}
}
