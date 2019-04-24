package main

import "net"
import "fmt"
import "bufio"
import "os"
import "io/ioutil"
import "strconv"
import "strings"
import "io"


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
  fmt.Printf(string(bbuf))
  conn.Write([]byte("\n"))
  conn.Write(bbuf)
  fmt.Println("=======================================================================")
  fmt.Println("Receive:")
  r := bufio.NewReader(conn)
  i, _ := r.ReadString('\n')
  lengthOfContent,_ := strconv.ParseUint(strings.Trim(i, "\n"), 10, 64)
  contentBuf := make([]byte, lengthOfContent)
  io.ReadFull(r, contentBuf)
  fmt.Println(string(contentBuf))
}

func main() {
//    SendFileToServer("create1.xml")
    SendFileToServer("acc_2.xml")
}
