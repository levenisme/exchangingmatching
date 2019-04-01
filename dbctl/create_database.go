package main

import(
	"database/sql"
	"fmt"
	//"strconv"
	"sync"
	//"log"
	_ "github.com/lib/pq"
)

var lock sync.Mutex

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
)

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
	activity += "order_id int,"
	activity += "price DECIMAL(32,2),"
	activity += "time timestamptz not null default now());"

	_,err = db.Exec(activity)

	//create table order_info 
	order := ""
	order += "CREATE TABLE order_info("
	order += "order_id int primary key,"
	order += "account_id int,"
	order += "open int,"
	order += "type int,"
	order += "amount DECIMAL(32,2),"
	order += "limit_price DECIMAL(32,2));"
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
	order_account += "ALTER TABLE order_info ADD CONSTRAINT constraint_fk FOREIGN KEY(account_id) REFERENCES account_info(accoUnt_id) ON DELETE CASCADE;"
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

func Insert_activity_info(db *sql.DB) {

}

func Insert_order_info(db *sql.DB) {
	
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

func main() {
	//connect to the database
	
	db,_ := Connect_database()
	fmt.Println("successfully connected!")
	Create_table(db)

	p,_ := Verify_account(db,"12345")
	fmt.Println(p)
	p,_ = Verify_account(db,"123")
	fmt.Println(p)
	p,_ = Verify_symbol(db,"abcd")
	fmt.Println(p)
	p,_ = Verify_symbol_account(db,"abcd","123","1234.0")
	fmt.Println(p)
	Insert_accout_info(db,"12345","1000")
	Insert_accout_info(db,"123","321")
	Insert_symbol_info(db,"abcd")
	Insert_account_to_symbol(db, "abcd", "123", "1234.0")
	p,_ = Verify_account(db,"12345")
	fmt.Println(p)
	p,_ = Verify_account(db,"123")
	fmt.Println(p)
	p,_ = Verify_symbol(db,"abcd")
	fmt.Println(p)
	p,_ = Verify_symbol_account(db,"abcd","123","1234.0")
	fmt.Println(p)
}
