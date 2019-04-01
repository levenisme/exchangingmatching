package main
import (
  "net"
 "fmt"
 "bufio"
 "time"
 "strconv"
 "io"
 "strings"
 "./xmlparser"
)

func GetLineFromConn(c chan string, r *bufio.Reader) {
  i, err := r.ReadString('\n')
  if err != nil {
    fmt.Println("v%",err)
    return
  }
  c <- i
  defer close(c)
}

func GetContentOfLength(c chan bool, r *bufio.Reader, length uint64, cbuf []byte) {
  bytesRead, err := io.ReadFull(r, cbuf)
  if err != nil || bytesRead != int(length) {
    c <- false
    return
  }
  c <- true
  defer close(c)
}



func HandleConnection(conn net.Conn){
  ///////////////////////////////////////////////////// initialize valriable
  r := bufio.NewReader(conn)
  incoming := make(chan string)
  readChan := make(chan bool)
  var contentBuf []byte
  // set a timeout
  timeout := time.After(5*time.Second)
  // initiate to read the first line
  var err error
  var lengthOfContent uint64


  go GetLineFromConn(incoming, r)
  //////////////////////////////// first select: read length of content with timeout
  select {
  case <- timeout:
    fmt.Println("Timed out")
    return
  case result := <- incoming:
    lengthOfContent,err = strconv.ParseUint(strings.Trim(result, "\n"), 10, 64)
    fmt.Println("length:%v", lengthOfContent)

    if err != nil {
      fmt.Println("%v", err)
      return
    }
    if lengthOfContent > 10000 {
      fmt.Println("the length of content is larger than 10000")
      return
    }
    contentBuf = make([]byte, lengthOfContent)
    go GetContentOfLength(readChan, r, lengthOfContent, contentBuf)
  }

  /////////////////////////////////// second select read content from conn with timeout
  select {
  case <- timeout:
    fmt.Println("Timed out")
    return
  case out:= <- readChan:
    if out == false {
      fmt.Println("Cannot receive exactly num of bytes")
      return
    }
  }


  //fmt.Println(contentBuf)
  var node xmlparser.Node
  err = xmlparser.GetXmlNode(contentBuf, &node)
  HandleXML(&node)
  fmt.Println("goroutine end")
  defer conn.Close()
}


func main() {
  fmt.Println("Launching server...")
  // listen on all interfaces
  ln, err := net.Listen("tcp", ":12345")
  if err != nil {
    fmt.Println(err)
    return
  }
  defer ln.Close()
  for {
    conn, err := ln.Accept()
    if err != nil {
      fmt.Println(err)
      return
    }
    go HandleConnection(conn)
  }
}
