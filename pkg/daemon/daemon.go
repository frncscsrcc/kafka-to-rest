package daemon

import (
	"kafka-to-rest/pkg/config"
	"kafka-to-rest/pkg/proxy"
	"log"
	"sync"
	"time"
)

type Daemon struct {
	proxies []*proxy.Proxy
	proxyWg sync.WaitGroup
}

func NewDaemon() *Daemon {
	return &Daemon{
		proxies: make([]*proxy.Proxy, 0),
	}
}

func NewDaemonFromConfig(c config.Config) *Daemon {
	d := &Daemon{
		proxies: make([]*proxy.Proxy, 0),
	}
	for _, proxyConfig := range c.Proxies {
		if p, err := proxy.NewProxyFromConfig(c.Group, proxyConfig, &d.proxyWg); err != nil {
			panic(err)
		} else {
			d.AddProxy(p)
		}
	}
	return d
}

func (d *Daemon) AddProxy(p *proxy.Proxy) error {
	d.proxies = append(d.proxies, p)
	d.proxyWg.Add(1)
	log.Printf("Added proxy %s, listen to %s", p.GetName(), p.GetTopics())
	return nil
}

func (d *Daemon) Start(initiateShutdown chan struct{}) int {
	log.Print("Starting the proxy")
	for _, p := range d.proxies {
		p.RunSync()
	}

	<-initiateShutdown
	log.Printf("Inititating graceful shutdown. Waiting %d seconds", 5)

	for _, p := range d.proxies {
		go p.Close()
	}

	gracefullShutdownDone := make(chan struct{})
	// Wait for the proxies to close
	go func() {
		d.proxyWg.Wait()
		gracefullShutdownDone <- struct{}{}
	}()

	// When for the waiting group, or the shutdown timer
	select {
	case <-gracefullShutdownDone:
		log.Print("All proxies were closed")
		return 0
	case <-time.NewTimer(5 * time.Second).C:
		log.Print("Some of the proxies were still open")
		return 2
	}

}
