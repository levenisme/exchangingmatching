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
 "./dbctl"
 "database/sql"
)

var db *sql.DB

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

func HandleXML (node *xmlparser.Node) (int, string) {
	if node == nil {
		return xmlparser.ERROR_NODE, "Nil node"
	}
	var ok int
	var ans string
	switch node.XMLName.Local {
	case "create" :
		HandleCreateNode(node)
	default :
		ok , ans = xmlparser.ERROR_NODE, "not known"
	}
	fmt.Println(ans)
	return ok, ans
}

func HandleSymNode (item *xmlparser.Node) {
  ok, ans = xmlparser.VerifySymNode(&item)
  sym := item.XMLName.Local
  if ok == xmlparser.VALID_NODE {
    symok, symerr = dbctl.Verify_symbol(sym)
    if symok == dbctl.INSERT {
      dbstl.Insert_symbol_info(db B,sym)
    }
    for _, sa_node := range item.Nodes {
      HandleSymAccountNode(&sa_node, sym)
    }
  }
  item.Rst = ans
  item.Rst_type = ok
  
}

func HandleAccountNode (item *xmlparser.Node) {
  ok, ans = xmlparser.VerifyActNode(&item)
  if ok == xmlparser.VALID_NODE {
    id, _ := item.AtrMap["id"]
    balance, _ := item.AtrMap["balance"]
    idans, iderr := dbctl.Verify_account(db, id)
    if idans == dbctl.INSERT {
      id_ist_err := dbctl.Insert_accout_info(db , id, balance)
      if id_ist_err != nil {
        ok = xmlparser.ERROR_NODE
        ans = "unknown database error 1 act node"
      }
    }
  }
  item.Rst = ans
  item.Rst_type = ok
}

func HandleSymAccountNode (item *xmlparser.Node, sym string) {
  sa_ok, sa_ans := xmlparser.VerifySymActNode(&item)
  if sa_ok == xmlparser.VALID_NODE {
    id, _ := item.AtrMap["id"]
    num := string(item.Content)
    idans, iderr := dbctl.Verify_symbol_account(db, sym, id)
    if idans == dbctl.INSERT {
      id_ist_err := dbctl.Insert_account_to_symbol(db, sym, id, num)
      if id_ist_err != nil {
        sa_ok = xmlparser.ERROR_NODE
        sa_ans = "unknown database error 1 symactnode"
      }
    } else if idans == dbctl.UPDATE {
      sa_ist_err := dbctl.Update_num_in_account_sym(db, num, id, sym)
      if sa_ist_err != nil {
        sa_ok = xmlparser.ERROR_NODE
        sa_ans = "unknown database error 2 symactnode"
      }
    } else {
      sa_ok = xmlparser.ERROR_NODE
      sa_ans = "unkniwn database error 3 symactnode"
    }
  }
  item.Rst = sa_ans
  item.Rst_type = sa_ok
}

func HandleCreateNode(crtNode *xmlparser.Node) (int, string){
  if crtNode == nil {
    return xmlparser.ERROR_NODE, "Error: nil create node"
  }
  nodeOK, nodeAns := xmlparser.VerifyNode(crtNode, &xmlparser.CrtFormat)
  if nodeOK == xmlparser.ERROR_NODE {
    return nodeOK, nodeAns
  }
  for _, item := range crtNode.Nodes {
    switch item.XMLName.Local {
    case "account" :
      HandleAccountNode(&item)
    case "symbol":
      HandleSymNode(&item)
    default:
      item.Rst = "unknown node"
      item.Rst_type = xmlparser.ERROR_NODE
    }
  }
  return xmlparser.VALID_NODE, ""
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
  var node xmlparser.Node
  err = xmlparser.GetXmlNode(contentBuf, &node)
  xmlparser.HandleXML(&node)
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
  var dberr error
  db, dberr = dbctl.Connect_database()
  if dberr != nil {
    fmt.Println(err)
    return
  }
  defer db.Close()
  
  for {
    conn, err := ln.Accept()
    if err != nil {
      fmt.Println(err)
      return
    }
    go HandleConnection(conn)
  }
}
