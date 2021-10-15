
import mysql.connector
import pandas as pd

'''Database Configuration Information'''

dbconfig={'host':'localhost',
        'user':'root',
        'password':'YOUR_PASSWORD',
        'auth_plugin':'mysql_native_password'}


'''SQL Sripts Data Definition Language '''

Create_Events_DB='CREATE DATABASE IF NOT EXISTS Event_Tickets;'

Use_Events_DB='USE Event_Tickets;'

Create_Ticket_Sales_TB='''CREATE TABLE IF NOT EXISTS Ticket_Sales(
    Ticket_id INT,
    Trans_date date,
    Event_id INT,
    Event_name VARCHAR(50),
    Event_date date,
    Event_type VARCHAR(50),
    Event_city VARCHAR(50),
    Customer_id INT,
    Price DECIMAL,
    Num_tickets INT);'''

''' CSV file path and column names are saved as variables to be used as input parameters '''

csv_path='/home/roger/SB/Ticket_System_Pipeline/third_party_sales.csv'
column_names=['Ticket_id','Trans_date','Event_id','Event_name','Event_date','Event_type','Event_city','Customer_id','Price','Num_tickets']

'''Creates database connection using database configuration inputs'''

def get_db_connection(): 
    try: 
        connection=mysql.connector.connect(
        host=dbconfig['host'],
        user=dbconfig['user'],
        password=dbconfig['password'],
        auth_plugin=dbconfig['auth_plugin'])
        return connection

    except Exception as error:
        print("Error while connecting to database for job tracker", error)

'''Executes DDL SQL Scripts'''

def create_db_schema(connection):
    try:
        if connection.is_connected():
            cursor=connection.cursor()
            cursor.execute(Create_Events_DB)
            cursor.execute(Use_Events_DB)
            cursor.execute(Create_Ticket_Sales_TB)
            connection.commit()
            cursor.close()
        
    except Exception as error:
        cursor.close()
        print("Error while creating database", error) 


'''Creates Pandas Dataframe from CSV file, and inserts each series of data into database'''

def load_csv_data(connection, path_to_csv):
    try:
        csv_data=pd.read_csv(path_to_csv,names=column_names,parse_dates=['Trans_date','Event_date'],dayfirst=True)
        if connection.is_connected():
            cursor=connection.cursor()
            for row in csv_data.itertuples():
                val=row.Ticket_id,row.Trans_date,row.Event_id,row.Event_name,row.Event_date,row.Event_type,row.Event_city,row.Customer_id,row.Price,row.Num_tickets
                load_query=f'''INSERT INTO Ticket_Sales (Ticket_id,Trans_date,Event_id,Event_name,Event_date,Event_type,Event_city,Customer_id,Price,Num_tickets) 
                            VALUES ({val[0]},'{val[1]}',{val[2]},{val[3]},'{val[4]}',{val[5]},{val[6]},{val[7]},{val[8]},{val[9]});'''
                cursor.execute(load_query)
            connection.commit()
            cursor.close()
    except Exception as error:
        cursor.close()
        print("Error while writing to table", error) 

'''Queries database for events by revenue, and transforms SQL result into Dataframe for visual display in terminal'''
def analyze_event_revenue(connection):
    try:
        if connection.is_connected():
            cursor=connection.cursor()
            analyze_query='''SELECT Event_type, 
                    SUM(Num_tickets) as Tickets_Sold,
                    CONCAT('$', SUM(Num_tickets * Price)) as Total_Revenue 
                    FROM Ticket_Sales 
                    GROUP BY Event_type 
                    ORDER BY Total_Revenue DESC;'''
            cursor.execute(analyze_query)
            result=cursor.fetchall()
            cursor.close()
        
            analysis=pd.DataFrame(result, columns=['Event_Type','Tickets_Sold', 'Total_Revenue'])
    
            print('\t [Analysis of Event Revenue]')
            print(analysis)

    except Exception as error:
        cursor.close()
        print("Error while querying table for analysis", error) 

if __name__ == "__main__":
    connection=get_db_connection()
    create_db_schema(connection)
    load_csv_data(connection,csv_path)
    analyze_event_revenue(connection)

