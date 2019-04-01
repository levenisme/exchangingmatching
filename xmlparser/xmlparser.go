package xmlparser

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"bytes"
	"unicode"
	"strings"
	"../dbctl"
)

type Node struct {
	XMLName xml.Name
	Attrs     []xml.Attr `xml:"-"`
	Content   []byte     `xml:",innerxml"`
	Nodes     []Node     `xml:",any"`
	Rst       string
	Rst_type  int
}

const (
	VALID_NODE = 0
	ERROR_NODE = 1
	FMT_NOT_REC = 0
	FMT_DECIMAL = 1
	FMT_NUMBER = 2
	
)

type RcqFormat struct{
	Type string
	Depth int
	Attr []string
	Attr_format []int
	Child []string
}

var ActFormat = RcqFormat{ "account", 1, []string{"id", "balance"}, []int{FMT_NUMBER, FMT_DECIMAL}, []string{} }

var SymActFormat = RcqFormat { "account" , 1, []string{"id"}, []int{FMT_NUMBER}, []string{} }

var SymFormat = RcqFormat {"symbol", 2, []string{"sym"},[]int{FMT_NOT_REC},[]string{"account"}}

var CrtFormat = RcqFormat {"create", 3, []string{}, []int{}, []string{"account", "symbol"}}

var TsctFormat = RcqFormat{"transactions", 2, []string{"id"}, []string{FMT_NUMBER}, []string{"order", "query", "cancel"}  }

var OdFormat RcqFormat { "order", 1, []string{"sym", "amount", "limit"}, []int {FMT_NOT_REC, FMT_DECIMAL, FMT_DECIMAL}, []string{}}

var QrFormat RcqFormat {"query", 1, []string{}, []int{}, []string{}}

var CcFormat RcqFormat {"cancel", 1, []string{}, []int{}, []string{}}


func (n *Node) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	n.Attrs = start.Attr
	type node Node
	return d.DecodeElement((*node)(n), &start)
}

func GetXmlNode(bbuf []byte, n *Node) (err error){
    buf := bytes.NewBuffer(bbuf)
	dec := xml.NewDecoder(buf)
	er := dec.Decode(n)
	if er != nil {
		return er
	}
	fmt.Println(n)
    return nil
}

func depthOfNode(n *Node ) int {
	if n == nil {
		return 0
	}
	ans := 0
	for _, ch := range n.Nodes {
		dOfCh := depthOfNode(&ch)
		if ans <  dOfCh {
			ans = dOfCh
		}
	}
	return ans + 1
}

func IsValidNumber(str string) bool {
	for _, c := range str {
        if !unicode.IsDigit(c) {
            return false
        }
	}
	return true
}

func IsValidDecimalNumber (str string) bool {
	if len(str) == 0 {
		return false
	}
	cnt := 0
	for _, c := range str {
		if c == '.' {
			cnt++
		} else if !unicode.IsDigit(c) {
			return false
		}
	}

	//return str[0] != '.' && str[len(str)-1] != '.' && (cnt <= 1 && len(str) - cnt <= dbctl.DECIMAL_LEN)
	return str[0] != '.' && str[len(str)-1] != '.' && (cnt <= 1 && len(str) - cnt <= 32)
}

func VerifyActNode (actNode *Node) (int, string) {
	return VerifyNode(actNode, &ActFormat)
}

func VerifySymActNode (symActNode *Node) (int, string) {
    ok, ans := VerifyNode(symActNode, &SymActFormat)
	if ok == VALID_NODE {
		if !IsValidDecimalNumber(strings.Trim(string(symActNode.Content), " ")) {
			return ERROR_NODE, "Error: account has invalid decimal position"
		}
	}
	return ok, ans
}

func VerifySymNode(symNode *Node) (int, string) {
	return VerifyNode(symNode, &SymFormat)
}
func VerifyCreateNode(crtNode *Node) (int, string){
	if crtNode == nil {
		return ERROR_NODE, "Error: nil create node"
	}
    nodeOK, nodeAns := VerifyNode(crtNode, &CrtFormat)
    if nodeOK == ERROR_NODE {
      return nodeOK, nodeAns
    }
    for _, item := range crtNode.Nodes {
		var ok int
		var ans string
		switch item.XMLName.Local {
		case "account" :
			ok, ans = VerifyActNode(&item)
		case "symbol":
			ok, ans = VerifySymNode(&item)
			var sa_ok int
			var sa_ans string
			for _, sa_node := range item.Nodes {
				sa_ok, sa_ans = VerifySymActNode(&sa_node)
				if sa_ok == ERROR_NODE {
					return sa_ok, sa_ans
				}
			}
		}
		if ok == ERROR_NODE {
			return ok, ans
		}
	}
	return VALID_NODE, ""
}

func VerifyNode (node *Node, rcq *RcqFormat) (int, string) {
	if node == nil {
		return ERROR_NODE, "Error:" + rcq.Type + " is nil"
	}
	if node.XMLName.Local != rcq.Type {
		return ERROR_NODE, "Error: This node is not " + rcq.Type + " " + node.XMLName.Local
	}
	if depthOfNode(node) > rcq.Depth {
		return ERROR_NODE, "Error:" + rcq.Type + " has invalid structure"
	}
	if len(node.Attrs) != len(rcq.Attr) {
		return ERROR_NODE, "Error: Number of " + rcq.Type + " attribute"
	} else {
        cntMap := make(map[string]int)
        fmtMap := make (map[string]int)
		for i,attr := range rcq.Attr {
			cntMap[attr] = 0
			fmtMap[attr] = rcq.Attr_format[i]
		}
		validCNT := 0
		for _,attr := range node.Attrs {
			mm, ok := cntMap[attr.Name.Local]
			if !ok {
				return ERROR_NODE, "Error: unknown attribute in the node"
			}
			if mm == 0 {
				cntMap[attr.Name.Local]++
				validCNT++
			} else {
				return ERROR_NODE, "Error: dulplicated attributes"
			}
            formt, ok := fmtMap[attr.Name.Local]
			switch formt {
			case FMT_DECIMAL:
				if !IsValidDecimalNumber(attr.Value) {
					return ERROR_NODE, "Error: Invalid attribute format"
				}
			case FMT_NUMBER:
				if !IsValidNumber (attr.Value) {
					return ERROR_NODE, "Error: Invalid attribute format"
				}
			default:

			}
		}
	}
    childMap := make(map[string]int)
	for _, fmt_child := range rcq.Child {
		childMap[fmt_child] = 1
	}
	for _,ch := range node.Nodes {
		_, ok := childMap[ch.XMLName.Local]
		if !ok {
			return ERROR_NODE, "Error: has invalid child node"
		}
	}
	return VALID_NODE, ""
}

func HandleXML (node &Node) (int, string) {
	if node == nil {
		return ERROR_NODE, "Nil node"
	}
	var ok int
	var ans string
	switch node.XMLName.Local {
	case "create" :
		ok,ans = VerifyCreateNode(node)
	default :
		ok , ans = ERROR_NODE, "not known"
	}
	fmt.Println(ans)
	return ok, ans
}

func main() {

    // Open our xmlFile
    xmlFile, err := os.Open("create.xml")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Successfully Opened users.xml")
	// defer the closing of our xmlFile so that we can parse it later on
	defer xmlFile.Close()

	// read our opened xmlFile as a byte array.
	bbuf, _ := ioutil.ReadAll(xmlFile)
    var n Node
	GetXmlNode(bbuf, &n)
	HandleXML(&n)

}
