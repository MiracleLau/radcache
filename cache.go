package radcache

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

type RadCache struct {
	Ctx     context.Context
	Db      *redis.Client
	Logger	*zap.SugaredLogger
	Options Options
}

type Options struct {
	Prefix string
}

func NewDefault() *RadCache {
	return &RadCache{
		Ctx: context.Background(),
		Options: Options{
			Prefix: "rad_",
		},
	}
}

func New(opt Options) *RadCache {
	return &RadCache{
		Ctx:     context.Background(),
		Options: opt,
	}
}

func (rad *RadCache) UseRedis(client *redis.Client) {
	rad.Db = client
}

func (rad *RadCache) UseZapLogger(logger *zap.SugaredLogger)  {
	rad.Logger = logger
}

// 序列化为json
func (rad *RadCache) Marshal(val interface{}) (string, error) {
	re, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(re), nil
}

// 从json反序列化
func (rad *RadCache) UnMarshal(val string) (interface{}, error) {
	var result interface{}
	err := json.Unmarshal([]byte(val), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 写入日志，如果未指定zap日志，则默认使用系统日志
func (rad *RadCache) Error(err interface{})  {
	if rad.Logger != nil {
		rad.Logger.Error(err)
	}else{
		log.Fatal(err)
	}
}

// 通用的设置值的方式
func (rad *RadCache) Set(key string, value interface{}, exp time.Duration) *redis.StatusCmd {
	val, err := rad.Marshal(value)
	if err != nil {
		cmd := redis.NewStatusCmd(rad.Ctx)
		cmd.SetErr(err)
		rad.Error(err)
		return cmd
	}
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, val, exp)
}

// 设置一个类型为string的缓存
func (rad *RadCache) SetString(key string, value string, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

func (rad *RadCache) SetInt(key string, value int, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

func (rad *RadCache) SetInt64(key string, value int64, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

func (rad *RadCache) SetBool(key string, value bool, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

func (rad *RadCache) SetFloat32(key string, value float32, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

func (rad *RadCache) SetFloat64(key string, value float64, exp time.Duration) *redis.StatusCmd {
	return rad.Db.Set(rad.Ctx, rad.Options.Prefix+key, value, exp)
}

// 通用的获取值的方式
func (rad *RadCache) Get(key string) (interface{}, error) {
	result, err := rad.Db.Get(rad.Ctx, rad.Options.Prefix+key).Result()
	if err != nil {
		rad.Error(err)
		return nil, err
	}
	return rad.UnMarshal(result)
}

func (rad *RadCache) GetString(key string) (string, error) {
	result, err := rad.Db.Get(rad.Ctx, rad.Options.Prefix+key).Result()
	if err != nil {
		rad.Error(err)
		return "", err
	}
	return result, nil
}

func (rad *RadCache) GetStringOrDefault(key string, val string) string {
	result,err := rad.GetString(key)
	if err != nil {
		return val
	}
	return result
}

func (rad *RadCache) GetInt(key string) (int,error) {
	result,err := rad.Db.Get(rad.Ctx,rad.Options.Prefix+key).Int()
	if err != nil {
		rad.Logger.Error(err)
		return -1, err
	}
	return result,nil
}

func (rad *RadCache) GetIntOrDefault(key string, val int) int {
	result,err := rad.GetInt(key)
	if err != nil {
		return val
	}
	return result
}

func (rad *RadCache) GetInt64(key string) (int64,error) {
	result,err := rad.Db.Get(rad.Ctx,rad.Options.Prefix+key).Int64()
	if err != nil {
		rad.Logger.Error(err)
		return -1, err
	}
	return result,nil
}

func (rad *RadCache) GetInt64OrDefault(key string, val int64) int64 {
	result,err := rad.GetInt64(key)
	if err != nil {
		return val
	}
	return result
}

func (rad *RadCache) GetBool(key string) (bool,error) {
	result,err := rad.Db.Get(rad.Ctx,rad.Options.Prefix+key).Bool()
	if err != nil {
		rad.Logger.Error(err)
		return false, err
	}
	return result,nil
}

func (rad *RadCache) GetBoolOrDefault(key string, val bool) bool {
	result,err := rad.GetBool(key)
	if err != nil {
		return val
	}
	return result
}

func (rad *RadCache) GetFloat32(key string) (float32,error) {
	result,err := rad.Db.Get(rad.Ctx,rad.Options.Prefix+key).Float32()
	if err != nil {
		rad.Logger.Error(err)
		return 0.0, err
	}
	return result,nil
}

func (rad *RadCache) GetFloat32OrDefault(key string, val float32) float32 {
	result,err := rad.GetFloat32(key)
	if err != nil {
		return val
	}
	return result
}

func (rad *RadCache) GetFloat64(key string) (float64,error) {
	result,err := rad.Db.Get(rad.Ctx,rad.Options.Prefix+key).Float64()
	if err != nil {
		rad.Logger.Error(err)
		return 0.0, err
	}
	return result,nil
}

func (rad *RadCache) GetFloat64OrDefault(key string, val float64) float64 {
	result,err := rad.GetFloat64(key)
	if err != nil {
		return val
	}
	return result
}

// 删除一个指定的缓存
func (rad *RadCache) Del(key string) error {
	err := rad.Db.Del(rad.Ctx, rad.Options.Prefix+key).Err()
	if err != nil {
		rad.Error(err)
	}
	return err
}

// 删除多个指定的缓存
func (rad *RadCache) DelAny(key ...string) error {
	var keys []string
	for _,v := range key {
		keys = append(keys,rad.Options.Prefix+v)
	}
	err := rad.Db.Del(rad.Ctx, keys...).Err()
	if err != nil {
		rad.Error(err)
	}
	return err
}

// 判断是否存在指定key
func (rad *RadCache) Exist(key string) bool {
	result := rad.Db.Exists(rad.Ctx,rad.Options.Prefix+key)
	if result.Val() == 1 {
		return true
	}else{
		return false
	}
}