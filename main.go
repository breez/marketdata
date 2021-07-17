package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tidwall/gjson"
)

const (
	yadioURL = "https://api.yadio.io/json"
)

var (
	redisPool *redis.Pool
)

func main() {
	redisConnect()
	getRates()
}

func redisConnect() error {
	db, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		return err
	}
	redisPool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", os.Getenv("REDIS_URL"), redis.DialDatabase(db))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return nil
}

func updateKeyFields(key string, ttl int, fields map[string]string) error {
	redisConn := redisPool.Get()
	defer redisConn.Close()
	if len(fields) > 0 {
		var args []interface{}
		args = append(args, key)
		for k, value := range fields {
			args = append(args, k)
			args = append(args, value)
		}
		_, err := redisConn.Do("HMSET", args...)
		if err != nil {
			log.Printf("Error in HMSET: %v", err)
		}
	}
	_, err := redisConn.Do("EXPIRE", key, ttl)
	return err
}

func getYadioRates() (map[string]string, error) {
	c := &http.Client{Timeout: 10 * time.Second}

	r, err := c.Get(yadioURL)
	if err != nil {
		return nil, fmt.Errorf("Get(%v): %w", yadioURL, err)
	}
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll(): %w", err)
	}
	json := string(body)
	rates := make(map[string]string)
	rates["VES"] = fmt.Sprintf("%f", gjson.Get(json, "VES.price").Float())
	usd := gjson.Get(json, "BTC.price").Float()
	rates["USD"] = fmt.Sprintf("%f", usd)
	bigUsd := big.NewFloat(usd)
	rates["EUR"] = fmt.Sprintf("%f", gjson.Get(json, "BTC.eur").Float())

	for _, currency := range []string{
		"COP", "CLP", "DOP", "UYU", "BRL", "PEN", "ARS", "MXN", "GBP", "RUB",
		"CNY", "JPY", "CAD", "AUD", "SGD", "CHF", "SEK", "KRW", "INR", "NOK",
		"TTD", "PYG", "TRY", "GTQ", "CRC", "ILS", "PAB", "VND", "AED", "HKD",
		"IDR", "DKK", "BOB", "NZD", "PHP", "CZK", "PLN", "PKR", "ZAR", "NAD",
		"RON", "ANG"} {
		usdCurrency := gjson.Get(json, "USD."+currency).Float()
		cur, _ := (new(big.Float).Mul(bigUsd, big.NewFloat(usdCurrency))).Float64()
		rates[currency] = fmt.Sprintf("%f", cur)
	}
	//log.Printf("%#v", rates)
	return rates, nil
}

func getRates() {
	for {
		rates, err := getYadioRates()
		if err != nil {
			log.Printf("Error in getYadioRates(): %v", err)
		} else {
			err = updateKeyFields("RATES:BTC", 600, rates)
			if err != nil {
				log.Printf("Error in updateKeyFields: %v", err)
			}
		}
		time.Sleep(30 * time.Second)
	}
}
