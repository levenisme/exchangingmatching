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
 "sync"
 "runtime"
 "bytes"
 "math"
 "container/list"
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

func HandleXML (node *xmlparser.Node) (string) {
	if node == nil {
		return "Nil node"
	}
	var ans string
	switch node.XMLName.Local {
	case "create" :
    ans = HandleCreateNode(node)
  case "transactions" :
    ans = HandleTransactionNode(node)
	default :
		ans = "not known"
	}
	fmt.Println(ans)
	return ans
}



func HandleSymNode (item *xmlparser.Node, response *bytes.Buffer) {
  ok, ans := xmlparser.VerifySymNode(item)
  if ok == xmlparser.VALID_NODE {
    sym := item.AtrMap["sym"]
    symok, _ := dbctl.Verify_symbol(db, sym)
    if symok == dbctl.INSERT {
      dbctl.Insert_symbol_info(db, sym)
    }
    for i:=0; i < len(item.Nodes); i++ {
      HandleSymAccountNode(&item.Nodes[i], sym, response)
    }
  }
  item.Rst = ans
  item.Rst_type = ok

}

func IsBuy(str string) bool {
  return str[0] != '-'
}



func HandleOrderNode(odNode *xmlparser.Node, account_id string) {
  ok, ans := xmlparser.VerifyOrderNode(odNode)
  if ok == xmlparser.VALID_NODE {

    amount := odNode.AtrMap["amount"]
    amt_temp, _ := strconv.ParseFloat(amount, 64)
    amount_v := math.Abs(amt_temp) // positive

    sym := odNode.AtrMap["sym"]

    limit := odNode.AtrMap["limit"]
    limit_v, _ := strconv.ParseFloat(limit, 64)

    position := dbctl.Get_position(db, account_id, sym)
    position_v, _ := strconv.ParseFloat(position, 64)

    is_buy := IsBuy(amount)
    if(is_buy) {
      balance := dbctl.Get_balance(db, account_id)
      balance_v, _ := strconv.ParseFloat(balance, 64)
      if balance_v - limit_v * amount_v <= - 0.005 {
        odNode.Rst = fmt.Sprintf("<error sym=\"%s\" amount=\"%s\" limit=\"%s\">Insufficient balance</error>\n", sym, amount,limit)
        return
      } else {
        dbctl.Add_num_balance_account_info(db, account_id, strconv.FormatFloat(- limit_v * amount_v, 'f', 2, 64 ))
      }
    } else {
      if position_v - amount_v <= -0.005 {
        odNode.Rst = fmt.Sprintf("<error sym=\"%s\" amount=\"%s\" limit=\"%s\">Insufficient position of this sym</error>\n", sym, amount,limit)
        return
      } else {
        dbctl.Add_num_number_acttosym(db, account_id, sym, amount)
      }
    }

    l := dbctl.Get_compare_info(db, sym, limit, is_buy )
    act_l := list.New()
    var income float64
    income = 0
    for e := l.Front(); e != nil && math.Abs(amount_v) >= 0.005 ; e = e.Next() {
      line := e.Value.([]string)
      target_tsct_id := line[0]
      target_num := line[1]
      target_price := line[2]
      target_account_id := line[3]

      target_num_temp, _ := strconv.ParseFloat(target_num, 64)
      target_num_v := math.Abs(target_num_temp) // positive
      target_price_v, _ := strconv.ParseFloat(target_price, 64)
      var diff float64
      if(amount_v > target_num_v) {
        diff = target_num_v
        amount_v -= target_num_v
        target_num_v = 0
      } else {
        diff = amount_v
        target_num_v -= amount_v
        amount_v = 0
      }
      var target_act_share_insert, cur_act_share_insert string

      if(is_buy) {
        target_act_share_insert = strconv.FormatFloat(0-diff, 'f', 2, 64 )
        cur_act_share_insert = strconv.FormatFloat(diff, 'f', 2, 64 )
        if(math.Abs(target_price_v - limit_v) >= 0.005) {
          income += math.Abs(target_price_v - limit_v) * diff
        }
        dbctl.Add_num_balance_account_info(db, target_account_id, strconv.FormatFloat(diff * target_price_v, 'f', 2, 64 ))
        dbctl.Add_num_open_order_info(db, target_tsct_id, strconv.FormatFloat(diff, 'f', 2, 64 )) // update 对方的order open（使用 -target_num_v）
        //dbctl.Update_open(db, strconv.FormatFloat(-target_num_v, 'f', 2, 64 ) , target_tsct_id)
      } else {
        target_act_share_insert = strconv.FormatFloat(diff, 'f', 2, 64 )
        cur_act_share_insert = strconv.FormatFloat(0-diff, 'f', 2, 64 )
        income += diff * target_price_v
        dbctl.Add_num_number_acttosym(db, target_account_id, sym, strconv.FormatFloat(diff, 'f', 2, 64 ))
        dbctl.Add_num_open_order_info(db, target_tsct_id, "-"+strconv.FormatFloat(diff, 'f', 2, 64 )  ) // update 对方的order open 使用 target_num_v
      }
      // insert target into activity table, update target balance in account_info, update balance
      dbctl.Insert_activity_info(db, target_tsct_id, target_price, target_act_share_insert)
      act_l.PushBack([]string{ target_price, cur_act_share_insert })
    }
    var open string
    if(is_buy) {
      open =strconv.FormatFloat(amount_v, 'f', 2, 64 )
    } else {
      open =strconv.FormatFloat(-amount_v, 'f', 2, 64 )
    }
    cur_order_id := dbctl.Insert_order_info(db, sym, account_id, open , amount, limit)
    // 双向更新之二（for）
    //
    for e := act_l.Front(); e != nil && math.Abs(amount_v) >= 0.005 ; e = e.Next() {
      line := e.Value.([]string)
      dbctl.Insert_activity_info(db, strconv.FormatInt(cur_order_id, 64), line[0], line[1] )
    }
    dbctl.Add_num_balance_account_info(db, account_id, strconv.FormatFloat(income, 'f', 2, 64 ))
    odNode.Rst = fmt.Sprintf("<opened sym=\"%s\" amount=\"%s\" limit=\"%s\" id=\"%d\">\n", sym, amount,limit,cur_order_id)
  } else {
    odNode.Rst_type = ok
    odNode.Rst = ans
  }


}

func HandleAccountNode (item *xmlparser.Node, response *bytes.Buffer) {
  ok, ans := xmlparser.VerifyActNode(item)
  var id string
  if ok == xmlparser.VALID_NODE {
    id, _ = item.AtrMap["id"]
    balance, _ := item.AtrMap["balance"]
    idans, _ := dbctl.Verify_account(db, id)
    if idans == dbctl.INSERT {
      id_ist_err := dbctl.Insert_accout_info(db , id, balance)
      if id_ist_err != nil {
        ok = xmlparser.ERROR_NODE
        ans = "unknown database error 1 act node"
      }
    } else {
      ok = xmlparser.ERROR_NODE
      ans = "Account already exisits"
    }
  }
  item.Rst = ans
  item.Rst_type = ok
  if(ok == xmlparser.ERROR_NODE) {
    response.WriteString( fmt.Sprintf("  <error id=\"%s\">%s</error>\n", id, ans) )
  } else {
    response.WriteString( fmt.Sprintf("  <created id=\"%s\"/>\n", id) )
  }
}

func HandleSymAccountNode (item *xmlparser.Node, sym string, response *bytes.Buffer) {
  sa_ok, sa_ans := xmlparser.VerifySymActNode(item)
  var id string
  if sa_ok == xmlparser.VALID_NODE {
    id, _ = item.AtrMap["id"]
    num := string(item.Content)
    idans, _ := dbctl.Verify_symbol_account(db, sym, id, num)
    if idans == dbctl.INSERT {
      id_ist_err := dbctl.Insert_account_to_symbol(db, sym, id, num)
      if id_ist_err != nil {
        sa_ok = xmlparser.ERROR_NODE
        sa_ans = "unknown database error 1 symactnode"
      }
    } else if idans == dbctl.UPDATE {
      //sa_ist_err :=
      dbctl.Update_num_in_account_sym(db, num, id, sym)
      //if sa_ist_err != nil {
        sa_ok = xmlparser.VALID_NODE
        sa_ans = ""
      //}
    } else {
      sa_ok = xmlparser.ERROR_NODE
      sa_ans = "Account doesn't exist in the database"
    }
  }
  if(sa_ok == xmlparser.ERROR_NODE) {
    response.WriteString(fmt.Sprintf("  <error sym=\"%s\" id=\"%s\">%s</error>\n", sym, id, sa_ans))
  } else {
    response.WriteString( fmt.Sprintf("  <created sym=\"%s\" id=\"%s\"/>\n",sym, id))
  }
  item.Rst = sa_ans
  item.Rst_type = sa_ok
}

// master goroutine wait for children go routine
func within(wg *sync.WaitGroup, f func(*xmlparser.Node, string), node *xmlparser.Node, account_id string) {
  wg.Add(1)
  go func() {
      defer wg.Done()

      f(node, account_id)
  }()
}



func HandleQueryNode(qrNode *xmlparser.Node, account_id string) {
  //fmt.Println("qqqqqqqqqqquery")
  qrNode.Rst = "I am query \n"
}

func HandleCancelNode(ccNode *xmlparser.Node, account_id string) {
  //fmt.Println("cccccccccccancel")
  ccNode.Rst = "I am cancel \n"
}

func CollectResponse( node *xmlparser.Node, response *bytes.Buffer) {
  if node == nil {
    return
  }
  response.WriteString (node.Rst)
}

func HandleTransactionNode(tsctNode *xmlparser.Node) (string) {
  if tsctNode == nil {
    return "Error: nil transaction node"
  }
  nodeOK, nodeAns := xmlparser.VerifyNode(tsctNode, &xmlparser.TsctFormat)

  if nodeOK == xmlparser.ERROR_NODE {
    return "<results>\n  <error>" + nodeAns + "</error>\n</results>"
  }
  account_id := tsctNode.AtrMap["id"]
  act_ok, _ := dbctl.Verify_account(db, account_id)
  if(act_ok == dbctl.INSERT) {
    return "<results>\n  <error> This account doesn't exist in the DB </error>\n</results>"
  }
  var wg sync.WaitGroup
  //fmt.Println(*tsctNode)
  //fmt.Println("%s  length: %d", tsctNode.XMLName.Local ,len(tsctNode.Nodes))
  for i:=0 ; i < len(tsctNode.Nodes) ; i++ {
    switch tsctNode.Nodes[i].XMLName.Local {
    case "order":
      within(&wg, HandleOrderNode, &tsctNode.Nodes[i], account_id)
    case "query":
      within(&wg, HandleQueryNode, &tsctNode.Nodes[i], account_id)
    case "cancel":
      within(&wg, HandleCancelNode, &tsctNode.Nodes[i], account_id)
    default:
      tsctNode.Nodes[i].Rst = "unknown node"
      tsctNode.Nodes[i].Rst_type = xmlparser.ERROR_NODE
    }
  }
  wg.Wait() // barrier
  var response bytes.Buffer
  response.WriteString("<result>\n")
  for i := 0; i < len(tsctNode.Nodes); i++  {
    CollectResponse( &tsctNode.Nodes[i], &response)
  }
  response.WriteString("</results>")
  return response.String()
}

func HandleCreateNode(crtNode *xmlparser.Node) ( string){
  if crtNode == nil {
    return  "Error: nil create node"
  }
  nodeOK, nodeAns := xmlparser.VerifyNode(crtNode, &xmlparser.CrtFormat)
  if nodeOK == xmlparser.ERROR_NODE {
    return  nodeAns
  }
  var response bytes.Buffer
  response.WriteString("<results>\n")
  for i:=0 ; i < len(crtNode.Nodes); i++ {
    switch crtNode.Nodes[i].XMLName.Local {
    case "account" :
      HandleAccountNode(&crtNode.Nodes[i], &response)
    case "symbol":
      HandleSymNode(&crtNode.Nodes[i], &response)
    default:
      crtNode.Nodes[i].Rst = "unknown node"
      crtNode.Nodes[i].Rst_type = xmlparser.ERROR_NODE
    }
  }
  response.WriteString("</results>")
  return response.String()
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
  HandleXML(&node)
  fmt.Println("goroutine end")
  defer conn.Close()
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
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
  dbctl.Create_table(db)
  for {
    conn, err := ln.Accept()
    if err != nil {
      fmt.Println(err)
      return
    }
    go HandleConnection(conn)
  }
}
