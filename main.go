package main

import (
	"log"

	"github.com/spf13/viper"
)

// Config ...
type Config struct {
	MaxTimeout       int
	MinTimeout       int
	NumberOfRequests int
	URLs             []string
}

func main() {

	log.Println(initConfig())

}

func initConfig() *Config {

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	err := viper.ReadInConfig()   // Find and read the config file
	if err != nil {               // Handle errors reading the config file
		log.Fatal(err)
	}

	return &Config{
		MaxTimeout:       viper.GetInt("MaxTimeout"),
		MinTimeout:       viper.GetInt("MinTimeout"),
		NumberOfRequests: viper.GetInt("NumberOfRequests"),
		URLs:             viper.GetStringSlice("URLs"),
	}
}
