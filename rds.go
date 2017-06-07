package rdsdownloader

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	mysqllog "github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/tkuchiki/aws-sdk-go-config"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var invalidMarkerErr = errors.New("invalid Marker")
var slowlogNotFoundErr = errors.New("slowquery log Not Found")

type rdsClient struct {
	client        *rds.RDS
	outputSlowlog string
}

func NewRDSClient(sess *session.Session, out string) rdsClient {
	return rdsClient{
		client:        rds.New(sess),
		outputSlowlog: out,
	}
}

func (r *rdsClient) createSlowlogName(f string) string {
	if r.outputSlowlog == "" {
		return filepath.Join(os.TempDir(), fmt.Sprintf("%s.slowquery.log", f))
	}

	return r.outputSlowlog
}

func (r *rdsClient) DescribeDBLogFiles(instanceID, filenameContains string) ([]*rds.DescribeDBLogFilesDetails, error) {
	params := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(instanceID),
		FilenameContains:     aws.String(filenameContains),
	}

	var files []*rds.DescribeDBLogFilesDetails
	resp, err := r.client.DescribeDBLogFiles(params)
	if err != nil {
		return files, err
	}

	sort.Slice(resp.DescribeDBLogFiles, func(i, j int) bool {
		return *resp.DescribeDBLogFiles[i].LastWritten > *resp.DescribeDBLogFiles[j].LastWritten
	})

	filesNum := len(resp.DescribeDBLogFiles)
	if filesNum > 2 {
		filesNum = 2
	}

	if filesNum == 0 {
		return files, slowlogNotFoundErr
	}

	files = make([]*rds.DescribeDBLogFilesDetails, filesNum)

	for i := 0; i < filesNum; i++ {
		files[i] = resp.DescribeDBLogFiles[i]
	}

	return files, nil
}

func (r *rdsClient) DownloadDBLogFilePortionPages(instanceID, logfile, marker string) (string, error) {
	req := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(instanceID),
		LogFileName:          aws.String(logfile), // Required
		Marker:               aws.String(marker),
	}

	var position string
	var tmpFp *os.File
	var slowlogErr error

	tmpFp, slowlogErr = ioutil.TempFile("", fmt.Sprintf("raw-%s-slowquery.log", instanceID))
	if slowlogErr != nil {
		return "", slowlogErr
	}
	os.Remove(tmpFp.Name())

	err := r.client.DownloadDBLogFilePortionPages(req,
		func(p *rds.DownloadDBLogFilePortionOutput, lastPage bool) bool {
			if p.LogFileData != nil {
				w := bufio.NewWriter(tmpFp)
				_, slowlogErr = w.WriteString(*p.LogFileData)
				if slowlogErr != nil {
					return true
				}
				w.Flush()
			}

			if lastPage == true {
				if p.Marker != nil {
					_, position, _ = parseMarker(*p.Marker)
				}
			}

			return *p.AdditionalDataPending
		})

	if slowlogErr != nil {
		return "", slowlogErr
	}

	var fp *os.File
	fp, slowlogErr = openSlowlog(r.createSlowlogName(instanceID))
	if slowlogErr != nil {
		return "", slowlogErr
	}
	defer fp.Close()

	_, seekerr := tmpFp.Seek(0, 0)
	if seekerr != nil {
		return "", seekerr
	}

	slp := parser.NewSlowLogParser(tmpFp, mysqllog.Options{})

	w := bufio.NewWriter(fp)

	go slp.Start()

	for e := range slp.EventChan() {
		if e.User == "rdsadmin" {
			continue
		}

		if strings.ToLower(e.Query) == "select @@version_comment limit 1" {
			continue
		}

		if strings.ToLower(e.Query) == "quit" {
			continue
		}

		_, slowlogErr = w.WriteString(outputSlowlog(e))
		if slowlogErr != nil {
			return "", slowlogErr
		}
		w.Flush()
	}

	return position, err
}

func NewAWSSession(accessKey, secretKey, arn, token, region string) (*session.Session, error) {
	conf := awsconfig.Option{
		Arn:       arn,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
		Token:     token,
	}

	return awsconfig.NewSession(conf)
}

func parseMarker(m string) (string, string, error) {
	markers := strings.Split(m, ":")
	if len(markers) < 2 {
		return "", "", invalidMarkerErr
	}

	return markers[0], markers[1], nil
}

func CreateMarker(m string) string {
	return fmt.Sprintf("%s:%s", m, m)
}

func SplitDBInstances(instances string) []string {
	splitedInstances := strings.Split(instances, ",")

	trimedInstances := make([]string, 0)

	for _, instance := range splitedInstances {
		trimedInstances = append(trimedInstances, strings.Trim(instance, " "))
	}

	return trimedInstances
}

func openSlowlog(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
}

func outputSlowlog(l *mysqllog.Event) string {
	var slowlog string
	if l.Ts != "" {
		slowlog = fmt.Sprintln(fmt.Sprintf("# Time: %s", l.Ts))
	}

	slowlog += fmt.Sprintln(
		fmt.Sprintf("# User@Host: %s[%s] @ %s []  Id: ", l.User, l.User, l.Host),
	)

	slowlog += fmt.Sprintln(
		fmt.Sprintf("# Query_time: %f Lock_time: %f Rows_sent: %d  Rows_examined: %d",
			l.TimeMetrics["Query_time"], l.TimeMetrics["Lock_time"],
			int64(l.NumberMetrics["Rows_sent"]), int64(l.NumberMetrics["Rows_examined"]),
		),
	)

	slowlog += fmt.Sprintln(l.Query)

	return slowlog
}
