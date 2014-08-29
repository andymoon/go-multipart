package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/cupcake/goamz/aws"
	"github.com/cupcake/goamz/s3"
	"io"
	"os"
	"path"
)

const (
	READ_BUFFER_SIZE  = 6000000
	WRITE_BUFFER_SIZE = 1000000
	S3_BUCKET         = ""
	AWS_KEY           = ""
	AWS_SECRET        = ""
	AWS_REGION        = ""
	AWS_ENDPOINT      = ""
)

var (
	fileName string
)

func main() {
	flag.StringVar(&fileName, "f", "", "File to upload")
	flag.Parse()
	if fileName == "" {
		fmt.Println("No file given")
		return
	}

	r, writer := io.Pipe()
	c := make(chan error)

	go s3Read(r, c)

	go writeFromFile(writer, c)

	err := <-c
	if err != nil {
		fmt.Println("error occured", err)
	}

	fmt.Println("Finished upload")
}

func getS3Bucket(bucketName string) (*s3.Bucket, error) {
	auth := aws.Auth{AWS_KEY, AWS_SECRET}
	region := aws.Region{Name: AWS_REGION, S3Endpoint: AWS_ENDPOINT}
	s3Client := s3.New(auth, region)
	bucket := s3Client.Bucket(bucketName)
	return bucket, nil
}

func writeFromFile(writer *io.PipeWriter, c chan error) {
	fileReader, err := os.Open(fileName)
	if err != nil {
		c <- err
		return
	}
	buf := make([]byte, WRITE_BUFFER_SIZE)
	i := 0
	for {
		var readCount int
		readCount, err = fileReader.Read(buf)
		if err == io.EOF {
			fmt.Println("file closed")
			writer.Close()
			break
		}

		count, err := writer.Write(buf[0:readCount])
		if err != nil {
			c <- err
			return
		}
		fmt.Printf("Write %d bytes\n", count)
		i++
	}
}

func fileRead(r *io.PipeReader, c chan error) {
	var buf = make([]byte, READ_BUFFER_SIZE)

	fileWriter, err := os.Create("/test/test.csv")

	if err != nil {
		fileWriter.Close()
		c <- err
		return
	}
	totalReadCount := 0
	for {
		fmt.Println("Wating to Read")
		fmt.Printf("Byte array length %d", len(buf))
		count, err := r.Read(buf)
		fmt.Printf("Read %d bytes\n", count)
		totalReadCount += count
		if err == io.EOF {
			fmt.Println("Reach end of file")
			if count > 0 {
				fileWriter.Write(buf[0 : totalReadCount-1])
			}
			fileWriter.Close()
			c <- nil
			break
		}

		if err != nil {
			fmt.Println("Error reading file", err)
			fileWriter.Close()
			c <- err
			break
		}

		if len(buf) >= READ_BUFFER_SIZE {
			fmt.Println("Writing to File")
			fileWriter.Write(buf[0 : totalReadCount-1])
			totalReadCount = 0
		}
	}
}

func s3Read(r *io.PipeReader, c chan error) {
	var buf = new(bytes.Buffer)
	var readBytes = make([]byte, READ_BUFFER_SIZE)
	bucket, err := getS3Bucket(S3_BUCKET)
	if err != nil {
		c <- err
		return
	}
	multi, err := bucket.Multi(path.Base(fileName), "text/plain", s3.Private)
	if err != nil {
		c <- err
		return
	}
	var parts []s3.Part
	var part s3.Part
	i := 1
	for {
		fmt.Println("Wating to Read")

		//The read will wait until there has been something written to the pipe.
		count, err := r.Read(readBytes)
		if count != 0 {
			buf.Write(readBytes[0:count])
		}

		fmt.Printf("Read %d bytes\n", count)

		if err == io.EOF {
			//If there is a buffer with length greater than zero than put that part.
			if buf.Len() > 0 {
				fmt.Printf("Putting %d bytes to s3\n", buf.Len())
				part, err = multi.PutPart(i, bytes.NewReader(buf.Bytes()))
				fmt.Println("Done putting to s3")
				fmt.Printf("Part size %d put to s3.", part.Size)
				if err != nil {
					multi.Abort()
					c <- err
					break
				}
				parts = append(parts, part)
			}

			if len(parts) > 0 {
				fmt.Printf("Completing s3 upload %d\n", len(parts))
				err = multi.Complete(parts)
				fmt.Println("Completed s3 upload")
				if err != nil {
					c <- err
					break
				}
			} else {
				fmt.Println("File not big enough for multipart")
				multi.Abort()
				if err := bucket.PutReader(path.Base(fileName), buf, int64(buf.Len()), "text/plain", s3.Private); err != nil {
					c <- err
					break
				}
			}
			fmt.Println("Finished Writing")
			c <- nil
			break
		}

		if err != nil {
			if multi != nil {
				multi.Abort()
			}
			fmt.Println("Reader errored", err)
			c <- err
			break
		}

		if buf.Len() >= READ_BUFFER_SIZE {

			part, err := multi.PutPart(i, bytes.NewReader(buf.Bytes()))

			if err != nil {
				fmt.Println("Error putting part", err)
				multi.Abort()
				c <- err
				return
			}
			fmt.Printf("Part size %d put to s3.", part.Size)
			parts = append(parts, part)
			i++
			buf.Reset()
		}
	}
}
