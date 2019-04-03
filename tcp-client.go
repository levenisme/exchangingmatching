package main

import "net"
import "fmt"
//import "bufio"
import "os"
import "io/ioutil"
import "strconv"

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
  defer conn.Close()
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
    //fmt.Println("length:%v", lengthOfContent)

    if err != nil {
      fmt.Println("%v", err)
      return
    }
    if lengthOfContent > 5000000 {
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
  var node xmlparser.Node
  err = xmlparser.GetXmlNode(contentBuf, &node)
  HandleXML(&node)
  fmt.Println("goroutine end")
  
}

func SendFileToServer (fileAddr string) {
  conn, err := net.Dial("tcp", "127.0.0.1:12345")
  if err != nil {
    return
  }
  fmt.Print("Text to send: ")
  xmlFile, err := os.Open(fileAddr)
  if err != nil{
    return
  }
  defer xmlFile.Close()
  bbuf, _ := ioutil.ReadAll(xmlFile)
  fmt.Println(len(bbuf))
  conn.Write([]byte( strconv.Itoa(len(bbuf))))
  fmt.Printf("%s",bbuf)
  conn.Write([]byte("\n"))
  conn.Write(bbuf)

  fmt.Println("Receive:")
  r := bufio.NewReader(conn)
  i, _ := r.ReadString('\n')
  lengthOfContent,_ = strconv.ParseUint(strings.Trim(i, "\n"), 10, 64)
  contentBuf = make([]byte, lengthOfContent)
  bytesRead, _ := io.ReadFull(r, contentBuf)
  fmt.Println("%s",string(contentBuf))
}

func main() {

    SendFileToServer("create.xml")
    SendFileToServer("transaction.xml")
}
