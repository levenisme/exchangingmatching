package dbctl

import(
	"database/sql"
	"fmt"
	"strconv"
	//"log"
	"time"
	"container/list"
	_ "github.com/lib/pq"
	"strings"
	"math"
)

const(
	host 	 = "localhost"
	port 	 = 5432
	user 	 = "postgres"
	password = "12345"
	dbname	 = "exchanging_matching"
)

const(
	ERROR  		= 0
	UPDATE 		= 1
	INSERT 		= 2
	DECIMAL_LEN = 32
	INSERT_TRANS_LEVEL = "REPEATABLE READ"
	READ_TRANS_LEVEL = "SERIALIZABLE"
	CANCELLED = 1
	EXECUTING = 2
	NOT_EXIST = -1
)

//edit num in order_info
func Add_num_open_order_info(db *sql.DB, order_id string, num string) {
	open := Get_open_or_caceltime(db, order_id, "open")
	to_add,_ := strconv.ParseFloat(open, 64)
	fmt.Print("to_add: ")
	fmt.Println(to_add)
	to_num,_ := strconv.ParseFloat(num, 64)
	to_add = to_add + to_num
	fmt.Print("to_add: ")
	fmt.Println(to_add)
	to_up := strconv.FormatFloat(to_add, 'f', 2, 64 )
	fmt.Print("to_up: ")
	fmt.Println(to_up)
	Update_open(db,to_up,order_id)	
}

//add number in acc_to_sym
func Add_num_number_acttosym(db *sql.DB, account_id string, symbol_id string, num string) {
	number,id := Get_number_acc_to_sym(db, account_id, symbol_id)
	fmt.Println("id")
	fmt.Println(id)
	to_add,_ := strconv.ParseFloat(number, 64)
	fmt.Print("to_add: ")
	to_num,_ := strconv.ParseFloat(num, 64)
	to_add = to_add + to_num
	fmt.Print("to_add: ")
	fmt.Println(to_add)
	to_up := strconv.FormatFloat(to_add, 'f', 2, 64 )
	fmt.Print("to_up: ")
	fmt.Println(to_up)
	Update_num_in_account_sym_in(db, id, to_up)

}

//add num to balance in account info
func Add_num_balance_account_info(db *sql.DB, account_id string, num string) {
	balance := Get_balance(db, account_id)
	to_add,_ := strconv.ParseFloat(balance, 64)
	fmt.Print("to_add: ")
	fmt.Println(to_add)
	to_num,_ := strconv.ParseFloat(num, 64)
	to_add = to_add + to_num
	fmt.Print("to_add: ")
	fmt.Println(to_add)
	to_up := strconv.FormatFloat(to_add, 'f', 2, 64 )
	fmt.Print("to_up: ")
	fmt.Println(to_up)
	Update_balance(db, to_up, account_id)

}
//get transaction_id(order_id), num, price from order_info
func Get_compare_info(db *sql.DB, sym string, limit string, is_buy bool) *list.List{
	query_buy := fmt.Sprintf("select order_id, open, limit_price, account_id from order_info where (symbol_id ='%s') and (type = 2 ) and (limit_price <= %s) and (open < 0 ) order by limit_price ASC, order_id ASC;", sym, limit)

	query_sell := fmt.Sprintf("select order_id, open, limit_price, account_id from order_info where (symbol_id ='%s') and (type = 2 ) and (limit_price >= %s) and (open > 0 ) order by limit_price DESC, order_id ASC;", sym, limit)
	
	var query string

	if is_buy {
		query = query_buy
	} else {
		query = query_sell
	}

	fmt.Println(query)
	rows,err := db.Query(query)

	l := list.New()
	for rows.Next(){
		var order_id, open,limit_price, account_id string
		rows.Columns()
		err = rows.Scan(&order_id, &open, &limit_price, &account_id)
		CheckErr(err)
		l.PushBack([]string{order_id, open, limit_price, account_id})
	}

	for e:= l.Front(); e != nil; e = e.Next(){
		fmt.Println(e.Value)
	}
	defer rows.Close()
	return l
}

//update number in account_to_sym
func Update_num_in_account_sym(db *sql.DB, num string, account_id string, symbol_id string) {
	update := "update account_to_symbol set number = "
	update = update + num + " where (account_id = '"
	update = update + account_id + "') and (symbol_id ='"
	update = update + symbol_id + "');"
	fmt.Println(update)
	db.Exec(update)
}

//update number in account_to_sym, need to change name, use internal 
func Update_num_in_account_sym_in(db *sql.DB, id string, num string) {
	update := "update account_to_symbol set number = "
	update = update + num + " where (account_symbol_id = "
	update = update + id + "); "
	fmt.Println(update)
	db.Exec(update)
}


//update balance in account_info
func Update_balance(db *sql.DB, balance string, account_id string) {
	update := "update account_info set balance = "
	update = update + balance + " where (account_id = '"
	update = update + account_id + "');"
	fmt.Println(update)
	db.Exec(update)
}

//update open in order_info
func Update_open(db *sql.DB, open string, order_id string) {
	update := "update order_info set open = "
	update = update + open + " where (order_id = "
	update = update + order_id + ");"
	fmt.Println(update)
	db.Exec(update)
}

//when cancel, update type and time in order_info
func Update_type_and_time(db *sql.DB, order_id string) {
	time_now := time.Now().Unix()
	update := "update order_info set type = "
	update = update + strconv.FormatInt(CANCELLED, 10) + ", time = " + strconv.FormatInt(time_now, 10) + " where order_id = " + order_id +"; "
	fmt.Println(update)
	db.Exec(update)
}

//check if account_id is 
func Authorize_account_order (db *sql.DB, account_id string, order_id string) bool {
	query := fmt.Sprintf("select account_id from order_info where order_id =%s;", order_id)
	row := db.QueryRow(query)
	var check_a string
	row.Scan(&check_a)
	return strings.Compare(check_a,account_id)==0
}
//get the num of the sym that an acount have
//if there is no num, it will return "" 
func Get_position(db *sql.DB, account_id string, sym string) string {
	pos := "select number from account_to_symbol where (account_id = '"
	pos += account_id
	pos += "') and (symbol_id = '"
	pos += sym
	pos += "') ;"
	fmt.Println(pos)
	row:= db.QueryRow(pos)
	var num string
	row.Scan(&num)
	fmt.Println(num)
	return num
}

//get balance of an account 
func Get_balance(db *sql.DB, account_id string) string {
	query := "select balance from account_info where account_id = '"
	query += account_id
	query += "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var balance string
	row.Scan(&balance)
	fmt.Println(balance)
	return balance
}

//get open shares or cancel time from order_info table, differentiated by input
func Get_open_or_caceltime(db *sql.DB, order_id string, check string) string {
	query := "select " + check + " from order_info where order_id = '"
	query += order_id 
	query += "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var req string
	row.Scan(&req)
	fmt.Println(req)
	return req
}

//get number from account_to_symbol
func Get_number_acc_to_sym(db *sql.DB, account_id string, symbol_id string) (string, string){
	query := fmt.Sprintf("select account_symbol_id, number from account_to_symbol where (account_id ='%s') and (symbol_id = '%s');", account_id,symbol_id)
	row := db.QueryRow(query)
	fmt.Println(query)
	var number,id string
	row.Scan(&id,&number)
	fmt.Println(number)
	fmt.Println(id)
	return number,id
}
//get type of the order, to check if it is cancelled
func Get_type(db *sql.DB, order_id string) int {
	query := "select type from order_info where order_id = '"
	query = query + order_id + "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var order_type int
	row.Scan(&order_type)
	fmt.Println("type")
	fmt.Println(order_type)
	if(order_type == CANCELLED){
		return CANCELLED
	}
	if(order_type == EXECUTING){
		return EXECUTING
	}
	return NOT_EXIST
}

//get type of the order, to check if it is cancelled
func Get_price(db *sql.DB, order_id string) string {
	query := "select limit_price from order_info where order_id = '"
	query = query + order_id + "'; "
	fmt.Println(query)
	row := db.QueryRow(query)
	var price string
	row.Scan(&price)
	//fmt.Println("type")
	//fmt.Println(order_type)
	return price
}

func Get_status_xml(db *sql.DB, order_id string) string{
	order_type := Get_type(db, order_id)
	open_shares := Get_open_or_caceltime(db, order_id,"open")
	open_shares_v,_ := strconv.ParseFloat(open_shares, 64)

	query := "select shares, price, activity_info.time from activity_info,order_info where (order_info.order_id = activity_info.order_id) and  (order_info.order_id = '"
	query += order_id
	query += "');"
	rows,_ := db.Query(query)
	defer rows.Close()
	var result string

	if(math.Abs(open_shares_v) >= 0.005) {
		if(order_type == CANCELLED) {
			time := Get_open_or_caceltime(db, order_id,"time")
			result += fmt.Sprintf("    <canceled shares=\"%s\" time=\"%s\"/>\n", open_shares, time)
		} else {
			result += fmt.Sprintf("    <open shares=\"%s\"/>\n", open_shares)
		}
	}
	for rows.Next(){
		var exe_shares,exe_price,exe_time string
		rows.Columns()
		_ = rows.Scan(&exe_shares, &exe_price, &exe_time)
		//CheckErr(err)
		result += fmt.Sprintf("    <executed shares=\"%s\" price=\"%s\" time=\"%s\"/>\n", exe_shares, exe_price, exe_time)
	}
	return result

}


func Create_table(db *sql.DB) {
	

	//drop table if exists
	_,err := db.Exec("DROP TABLE IF EXISTS order_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS activity_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS account_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS symbol_info cascade;")
	_,err = db.Exec("DROP TABLE IF EXISTS account_to_symbol cascade;")
	if err != nil{
		panic(err)
	}

	//create table activity_info
	activity := ""
	activity += "CREATE TABLE activity_info("
	activity += "activity_id serial primary key,"
	activity += "shares int,"
	activity += "order_id int,"
	activity += "price DECIMAL(32,2),"
	activity += "time bigint);"
	fmt.Println(activity)
	_,err = db.Exec(activity)

	//create table order_info 
	order := ""
	order += "CREATE TABLE order_info("
	order += "order_id serial primary key,"
	order += "symbol_id text,"//foreign key?
	order += "account_id text,"
	order += "open DECIMAL(32,2),"
	order += "type int,"
	order += "amount DECIMAL(32,2),"
	order += "limit_price DECIMAL(32,2),"
	order += "time bigint);"
	fmt.Println(order)
	_,err = db.Exec(order)
	
	//create table account_info 
	account := ""
	account += "CREATE TABLE account_info("
	account += "account_id text primary key,"
	account += "balance DECIMAL(32,2));"
	_,err = db.Exec(account)

	//create table symbol_info 
	symbol := ""
	symbol += "CREATE TABLE symbol_info("
	symbol += "symbol_id text primary key);"
	_,err = db.Exec(symbol)

	//create foreign key between order and account
	order_account := ""
	order_account += "ALTER TABLE order_info ADD CONSTRAINT constraint_fk FOREIGN KEY(account_id) REFERENCES account_info(account_id) ON DELETE CASCADE;"
	_,err = db.Exec(order_account);

	//create foreign key between activity and order
	activity_order := ""
	activity_order += "ALTER TABLE activity_info ADD CONSTRAINT constraint_fk FOREIGN KEY(order_id) REFERENCES order_info(order_id) ON DELETE CASCADE;"
	_,err = db.Exec(activity_order);

	//build many-to-many relationship between account and symbol
	account_symbol := ""
	account_symbol += "CREATE TABLE account_to_symbol("
	//account_symbol += "account_symbol_id int primary key,"
	account_symbol += "account_symbol_id serial primary key,"
	account_symbol += "account_id text references account_info(account_id), "
	account_symbol += "symbol_id text references symbol_info(symbol_id),"
	account_symbol += "number DECIMAL(32,2));"
	_,err = db.Exec(account_symbol);
	if err != nil{
		panic(err)
	}
}

func CheckCount(rows *sql.Rows) (count int) {
 	for rows.Next() {
    	err:= rows.Scan(&count)
    	CheckErr(err)
    }   
    return count
}

func CheckErr(err error) {
    if err != nil {
        panic(err)
    }
}

//check if symbol is in symbol table, if not, could insert
func Verify_symbol(db *sql.DB, symbol_id string) (int,error){
	check_s := ""
	check_s +="select count(symbol_id) from symbol_info where symbol_id='"
	check_s += symbol_id
	check_s += "'"
	fmt.Println(check_s)

	rows,err := db.Query(check_s)
	count := CheckCount(rows)
	if(count!=0){
		return ERROR,err
	}

	defer rows.Close()
	return INSERT,err
}

//check if account is in account table, if not, could insert
func Verify_account(db *sql.DB, account_id string) (int, error) {
	check_a := ""
	check_a +="select count(account_id) from account_info where account_id='"
	check_a +=account_id
	check_a +="'; "
	fmt.Println(check_a)

	rows,err := db.Query(check_a)
	count := CheckCount(rows)
	if(count!=0){
		return ERROR,err
	}
	defer rows.Close()
	return INSERT,err

}

//check id account have a specific symbol, id account not exist, return err, if exist but have
//no sym, return insert, it both exists, return update
func Verify_symbol_account(db *sql.DB, symbol_id string, account_id string, num string) (int, error) {
	//check if account_id exists,if not, return 0, error
	check_a := ""
	check_a +="select count(account_id) from account_info where account_id='"
	check_a +=account_id
	check_a +="'; "
	fmt.Println(check_a)

	rows,err := db.Query(check_a)

	count := CheckCount(rows)
	if(count == 0){
		return ERROR,err
	}

	//account_id exists, check if it is in symbol_account, if exists, update and return 1, if not, return 2
	check_sym := ""
	check_sym +="select count(account_id) from account_to_symbol where account_id='"
	check_sym +=account_id
	check_sym +="'; "

	rows_sym, err := db.Query(check_sym)

	count = CheckCount(rows_sym)
	if(count!=0){
		return UPDATE,err
	}
	defer rows.Close()
	defer rows_sym.Close()
	return INSERT,err
}


func Insert_accout_info(db *sql.DB, account_id string, balance string) error{
	insert := ""
	insert += "insert into account_info values ('"
	insert += account_id
	insert += "', '"
	insert += balance
	insert += "'); " 
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err

}

func Insert_account_to_symbol(db *sql.DB, sym string, account_id string, num string) error{
	insert := ""
	insert += "insert into account_to_symbol(symbol_id,account_id,number) values ('"
	insert += sym
	insert += "', '"
	insert += account_id
	insert += "', '"
	insert += num
	insert += "');"
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err
}

func Insert_activity_info(db *sql.DB, order_id string, price string, shares string) {
	insert := "insert into activity_info(order_id,shares,price,time) values ("
	time_now := time.Now().Unix()
	insert = insert + order_id + ", " + shares + ", " + price + ", " + strconv.FormatInt(time_now,10) + "); "
	fmt.Println(insert)
	db.Exec(insert)
}

func Insert_order_info(db *sql.DB, sym string, account_id string, open string, amount string, limit string) int64 {
	query := "insert into order_info(symbol_id, account_id, open, type, amount, limit_price) values ('"
	query = query + sym + "', '" + account_id + "', " + open + ", " + strconv.FormatInt(EXECUTING, 10) + ", "+amount +", " + limit + ") returning order_id; "
	fmt.Println(query)
	var insert_id int64
	//row := db.Exec(query)
	//get_order_id := fmt.Sprintf("select order_id from order_info where (symbol_id ='%s') and (type = 2 ) and (account_id = '%s') and (limit_price = %s) and (open = %s ) ", sym, account_id, limit)

	row := db.QueryRow(query)
	row.Scan(&insert_id)
	return insert_id
}

func Insert_symbol_info(db *sql.DB,sym string) error{
	insert := ""
	insert += "insert into symbol_info values ('"
	insert += sym
	insert +="');"
	fmt.Println(insert)
	_,err := db.Exec(insert)
	return err
}

func Connect_database() (db *sql.DB, errstr error){
	psqlInfo :=  fmt.Sprintf("host=%s port=%d user=%s "+
		"password = %s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)

	if err != nil{
		fmt.Println("\nconnection mysql error")
		return db, err
		panic(err)
	}
	//defer db.Close()
	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	err = db.Ping()

	if err != nil{
		fmt.Println("\nopen mysql error ", err)
		return db, err
	}	
	

	return db,nil
}

func BeginTransaction(db *sql.DB) {
	db.Exec("begin transaction isolation level repeatable read;")
}

func TransCommit(db *sql.DB) {
	db.Exec("commit;")
}
func main() {
	//connect to the database
	
	db,_ := Connect_database()
	fmt.Println("successfully connected!")
	Create_table(db)
 	//Insert_accout_info()
 	BeginTransaction(db)
	Insert_accout_info(db, "111", "1000000")
	Insert_accout_info(db, "112", "1000000")
	Insert_accout_info(db, "113", "1000000")
	Insert_accout_info(db, "114", "1000000")
	Insert_accout_info(db, "115", "1000000")
	Insert_symbol_info(db,"ABC")
	Insert_account_to_symbol(db, "ABC", "111", "100")
	Insert_account_to_symbol(db, "ABC", "112", "100")
	Insert_account_to_symbol(db, "ABC", "113", "100")
	Insert_account_to_symbol(db, "ABC", "114", "100")
	Insert_account_to_symbol(db, "ABC", "115", "100")
	order_id := Insert_order_info(db, "ABC", "111", "-500","-500", "125")
	order_id = Insert_order_info(db, "ABC", "112", "-700","-700", "120")
	get_order_id := strconv.FormatInt(order_id, 10)
	Add_num_open_order_info(db, get_order_id, "200")
	Insert_account_to_symbol(db, "ABC", "111", "50")
	Add_num_number_acttosym(db, "111", "ABC", "20")
	Add_num_balance_account_info(db, "111", "-800")
	TransCommit(db)
	
}
