package main

import "net"
import "fmt"
//import "bufio"
import "os"
import "io/ioutil"
import "strconv"

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
}

func main() {
    SendFileToServer("transaction.xml")
}
