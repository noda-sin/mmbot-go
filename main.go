package main

import (
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/kkohtaka/go-bitflyer/pkg/api/realtime"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1/board"
	"github.com/kkohtaka/go-bitflyer/pkg/api/v1/markets"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	debug = flag.Bool("debug", true, "The Mode to output debug log.")

	ProductCodes = []markets.ProductCode{
		"FX_BTC_JPY",
	}
)

func main() {
	flag.Parse()
	logInit(*debug)

	ticker := time.NewTicker(time.Second * 5)

	ch, err := Load()

	if err != nil {
		log.Fatalln(err)
		return
	}

	go func() {
		for range ticker.C {
			book := <-ch
			fmt.Println(book)
		}
	}()
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	ticker.Stop()
}

func Load() (chan board.Response, error) {
	c := realtime.NewClient()
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.MaxInterval = 15.0 * time.Minute
	exponentialBackoff.MaxElapsedTime = 0.0

	ch := make(chan board.Response)

	go func() {
		err := backoff.Retry(
			func() error {
				sess, err := c.Connect()
				if err != nil {
					return err
				}

				sub := realtime.NewSubscriber()
				sub.HandleOrderBook(ProductCodes,
					func(response board.Response) error {
						func() {
							ch <- response
						}()
						return nil
					},
				)
				err = sub.ListenAndServe(sess)
				log.WithError(err).Warn("connection closed")
				return errors.Wrap(err, "connection closed")
			},
			exponentialBackoff,
		)
		if err != nil {
			log.WithError(err).Fatalln("connect realtime API")
		}
	}()

	return ch, nil
}

func logInit(debug bool) {
	format := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	}
	log.SetFormatter(format)
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.SetOutput(os.Stdout)
}
