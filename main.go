package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/lucazulian/cryptocomparego"
	"github.com/lucazulian/cryptocomparego/context"
)

var (
	redisPool *redis.Pool
)

func main() {
	redisConnect()
	client := cryptocomparego.NewClient(nil)
	ctx := context.TODO()
	getRates(ctx, client)
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
	_, err = redisConn.Do("EXPIRE", key, ttl)
	return err
}

func getRates(ctx context.Context, client *cryptocomparego.Client) {
	for {
		priceRequest := cryptocomparego.NewPriceRequest("BTC", []string{"USD", "EUR", "GBP", "JPY"})
		priceList, _, err := client.Price.List(ctx, priceRequest)

		if err != nil {
			log.Panicf("Error in PriceMulti.List: %v", err)
		}

		rates := make(map[string]string)
		for _, coin := range priceList {
			rates[coin.Name] = fmt.Sprintf("%f", coin.Value)
		}
		err = updateKeyFields("RATES:BTC", 600, rates)
		if err != nil {
			log.Panicf("Error in updateKeyFields: %v", err)
		}
		time.Sleep(30 * time.Second)
	}
}
