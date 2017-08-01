package main

import (
	"github.com/tkuchiki/rds-slowlog-downloader"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"path/filepath"
)

var (
	defaultConfig      = filepath.Join(os.TempDir(), "rds-slowlog-downloader.conf")
	defaultSlowlog     = filepath.Join(os.TempDir(), "instance_id.slowquery.log")
	awsAccessKeyID     = kingpin.Flag("access-key", "AWS access key ID").String()
	awsSecretAccessKey = kingpin.Flag("secret-key", "AWS secret access key").String()
	arn                = kingpin.Flag("arn", "AWS Assume role ARN").String()
	token              = kingpin.Flag("token", "AWS access token").String()
	profile            = kingpin.Flag("profile", "AWS CLI profile").String()
	creds              = kingpin.Flag("credentials", "AWS CLI Credential file").String()
	region             = kingpin.Flag("region", "AWS region").String()
	awsConfig          = kingpin.Flag("aws-config", "AWS CLI Config file").String()
	instanceIDs        = kingpin.Flag("instance-ids", "RDS instance IDs (comma separated)").String()
	configFile         = kingpin.Flag("config-file", "Config(Position) file").Default(defaultConfig).String()
	outputSlowlog      = kingpin.Flag("output", "Output to slowlog").PlaceHolder(defaultSlowlog).String()
)

func main() {
	kingpin.Version("0.1.0")
	kingpin.Parse()

	sess, serr := rdsdownloader.NewAWSSession(*awsAccessKeyID, *awsSecretAccessKey, *arn, *token, *region)
	if serr != nil {
		log.Fatal(serr)
	}

	rdsClient := rdsdownloader.NewRDSClient(sess, *outputSlowlog)

	configExists := true
	currentConf, lerr := rdsdownloader.LoadConfig(*configFile)
	if lerr != nil {
		configExists = false
		currentConf = make(rdsdownloader.Positions, 0)
	}

	defaultMarker := "0:0"

	dbInstancesStr := *instanceIDs

	dbInstances := rdsdownloader.SplitDBInstances(dbInstancesStr)

	newConf := rdsdownloader.Positions{}

	for _, dbInstance := range dbInstances {
		files, ferr := rdsClient.DescribeDBLogFiles(dbInstance, "slowquery")
		if ferr != nil {
			log.Fatal(ferr)
		}

		var prevLogfile string
		if len(files) == 1 {
			prevLogfile = ""
		} else {
			prevLogfile = *files[1].LogFileName
		}
		currentLogfile := *files[0].LogFileName

		var currentMarker string
		var logerr error

		if configExists {
			position := currentConf[dbInstance]
			if position.PrevLogfile != "" && position.PrevLogfile != prevLogfile { // after rotate
				// rotated slowlog
				_, logerr = rdsClient.DownloadDBLogFilePortionPages(
					dbInstance, prevLogfile, rdsdownloader.CreateMarker(position.Marker))

				if logerr != nil {
					log.Println(logerr.Error())
				}

				// current slowlog
				currentMarker, logerr = rdsClient.DownloadDBLogFilePortionPages(
					dbInstance, currentLogfile, defaultMarker)
			} else {
				currentMarker, logerr = rdsClient.DownloadDBLogFilePortionPages(
					dbInstance, currentLogfile, rdsdownloader.CreateMarker(position.Marker))
			}

			if currentMarker == "" {
				currentMarker = position.Marker
			}
		} else {
			currentMarker, logerr = rdsClient.DownloadDBLogFilePortionPages(dbInstance, currentLogfile, defaultMarker)
		}

		if logerr != nil {
			log.Println(logerr.Error())
		}

		newConf[dbInstance] = rdsdownloader.Position{
			PrevLogfile: prevLogfile,
			LastWritten: *files[0].LastWritten,
			Size:        *files[0].Size,
			Marker:      currentMarker,
		}
	}

	if !rdsdownloader.CmpPosition(currentConf, newConf) {
		jerr := rdsdownloader.WriteConfig(*configFile, newConf)
		if jerr != nil {
			log.Fatal(jerr)
		}
	}
}
