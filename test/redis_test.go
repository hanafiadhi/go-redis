package test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)
 var RedisClient = redis.NewClient(&redis.Options{
	Addr:	  "localhost:6379",
	Password: "", // no password set
	DB:		  0,  // use default DB
})

func TestConnectionRedis(t *testing.T)  {
	assert.NotNil(t,RedisClient)
	err := RedisClient.Close()
	assert.Nil(t,err)
}

var ctx = context.Background()

func TestPing(t *testing.T)  {
	result, err:= RedisClient.Ping(ctx).Result()
	assert.Nil(t,err)
	assert.Equal(t,"PONG",result)
}

func TestString(t *testing.T){
	RedisClient.SetEx(ctx,"name","Hanafi Adhi",time.Second*3)
	result, err := RedisClient.Get(ctx,"name").Result()
	assert.Nil(t,err)
	assert.Equal(t,"Hanafi Adhi",result)

	time.Sleep(time.Second * 5)
	_, err = RedisClient.Get(ctx,"name").Result()
	assert.NotNil(t,err)
}

func TestList(t *testing.T)  {
	RedisClient.RPush(ctx,"names","hanafi")
	RedisClient.RPush(ctx,"names","adhi")
	RedisClient.RPush(ctx,"names","prasetyo")

	assert.Equal(t,"hanafi",RedisClient.LPop(ctx,"names").Val())

	assert.Equal(t,"adhi",RedisClient.LPop(ctx,"names").Val())

	assert.Equal(t,"prasetyo",RedisClient.LPop(ctx,"names").Val())
}

func TestSet(t *testing.T)  {
	RedisClient.SAdd(ctx,"students","Hanafi")
	RedisClient.SAdd(ctx,"students","Adhi")
	RedisClient.SAdd(ctx,"students","Prasetyo")

	assert.Equal(t,int64(3),RedisClient.SCard(ctx,"students").Val())
	assert.Equal(t,[]string{"Hanafi","Prasetyo","Adhi"}, RedisClient.SMembers(ctx,"students").Val())
	RedisClient.Del(ctx,"students")
}

func TestSortedSet(t *testing.T)  {
	RedisClient.ZAdd(ctx,"scores",redis.Z{Score: 100, Member: "Hanafi"})
	RedisClient.ZAdd(ctx,"scores",redis.Z{Score: 85, Member: "Prasetyo"})
	RedisClient.ZAdd(ctx,"scores",redis.Z{Score: 95, Member: "Adhi"})

	assert.Equal(t, []string{"Prasetyo","Adhi","Hanafi"}, RedisClient.ZRange(ctx,"scores",0,2).Val())
	assert.Equal(t,"Hanafi",RedisClient.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Adhi",RedisClient.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Prasetyo",RedisClient.ZPopMax(ctx,"scores").Val()[0].Member)
}

func TestHash(t *testing.T)  {
	RedisClient.HSet(ctx,"user:2","id","1")
	RedisClient.HSet(ctx,"user:2","name","Hanafi Adhi Prasetyo")
	RedisClient.HSet(ctx,"user:2","email","hanafiadhi@gmail.com")

	user := RedisClient.HGetAll(ctx,"user:2").Val()
	assert.Equal(t,"1", user["id"])
	assert.Equal(t,"Hanafi Adhi Prasetyo", user["name"])
	assert.Equal(t,"hanafiadhi@gmail.com", user["email"])
}

func TestGeoPoint(t *testing.T)  {
	RedisClient.GeoAdd(ctx,"sellers",&redis.GeoLocation{
		Name: "Toko A",
		Longitude: 106.82669941006166,
		Latitude: -6.175594179868375,
	})
	RedisClient.GeoAdd(ctx,"sellers",&redis.GeoLocation{
		Name: "Toko B",
		Longitude: 106.819526,
		Latitude: 	-6.181777,
	})

	distance := RedisClient.GeoDist(ctx,"sellers","Toko A", "Toko B","km").Val()
	assert.Equal(t,1.0503,distance)

	sellers := RedisClient.GeoSearch(ctx, "sellers",&redis.GeoSearchQuery{
		Longitude: 106.82188806612915,
		Latitude: -6.184288347247865,
		Radius: 5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t,[]string{"Toko B","Toko A"},sellers)
}

func TestPipeLine(t *testing.T)  {
	 _, err :=RedisClient.Pipelined(ctx,func(pipeline redis.Pipeliner) error {
		// Lakukan operasi Redis di dalam fungsi anonim ini
		pipeline.SetEx(ctx,"name", "Hanafi", 5*time.Second)
		pipeline.SetEx(ctx, "occupation", "Software Engineer", 5*time.Second)
		return nil
	})
	assert.Nil(t,err)
	assert.Equal(t,"Hanafi",RedisClient.Get(ctx,"name").Val())
	assert.Equal(t,"Software Engineer",RedisClient.Get(ctx,"occupation").Val())
}

func TestTransaction(t *testing.T)  {
	_,err := RedisClient.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetEx(ctx,"name","Hanafi",10 * time.Second)
		p.SetEx(ctx, "occupation", "Software Engineer", 10*time.Second)
		return nil
	})
	assert.Nil(t,err)
	assert.Equal(t,"Hanafi",RedisClient.Get(ctx,"name").Val())
	assert.Equal(t,"Software Engineer",RedisClient.Get(ctx,"occupation").Val())
}
