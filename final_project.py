### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows





def create_data_dic(data_filename):
    # custom made function
    # Inputs: Name of the data 
    # Output: returns data dic with key as headers and rows for those headers as values

    ### BEGIN SOLUTION
    #reading csv file and adding lines to a list. The seprate used is ",", please change if using on
    # anyother file
    header = ''
    data = []
    with open(data_filename, 'r') as file:
        for lines in file:
            if not lines.strip():
                continue
            if not header:
                header=lines.split(',')
                continue
            data.append(lines.split(","))

    ## creating a dic for headers and data
    data_dic = {}
    for count1, i in enumerate(header):
        inli = []
        for count2,j in enumerate(data):
            inli.append(j[count1].strip())
        data_dic[i.strip()] = inli

    return data_dic
    ### END SOLUTION



def step1_create_program_category_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    pgm_catgrs = []
    for line in data_dic["Program Category"]:
        if line not in pgm_catgrs:
            pgm_catgrs.append(line)
    pgm_catgrs.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [ProgramCategory] (
    [ProgramID] Integer not null primary key,
    [Program] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "ProgramCategory")


    def insert_values(conn, values):
        sql = """INSERT INTO ProgramCategory(Program)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in pgm_catgrs:
            #print(count)
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step1_create_program_to_programid_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_program="SELECT Program FROM ProgramCategory"
    programs = execute_sql_statement(sql_select_program, conn)
    programs=list(map(lambda row: row[0].strip(), programs))
    conn.close()
    our_dict={}
    count=1
    for i in programs:
        our_dict[i]=count
        count+=1
    return our_dict

def step2_create_regions_served_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Region Served"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [RegionsServed] (
    [RegionID] Integer not null primary key,
    [Region] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "RegionsServed")


    def insert_values(conn, values):
        sql = """INSERT INTO RegionsServed(Region)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_region="SELECT Region FROM RegionsServed"
    regions = execute_sql_statement(sql_select_region, conn)
    regions=list(map(lambda row: row[0].strip(), regions))
    conn.close()
    our_dict={}
    count=1
    for i in regions:
        our_dict[i]=count
        count+=1
    return our_dict

def step3_create_sex_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Sex"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [SexTable] (
    [SexID] Integer not null primary key,
    [SexValue] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "SexTable")


    def insert_values(conn, values):
        sql = """INSERT INTO SexTable(SexValue)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step3_create_sex_to_sexid_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_sex="SELECT SexValue FROM SexTable"
    sexvalues = execute_sql_statement(sql_select_sex, conn)
    sexvalues=list(map(lambda row: row[0].strip(), sexvalues))
    conn.close()
    our_dict={}
    count=1
    for i in sexvalues:
        our_dict[i]=count
        count+=1
    return our_dict

def step4_living_sit_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Living Situation"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [LivingSit] (
    [ID] Integer not null primary key,
    [Value] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "LivingSit")


    def insert_values(conn, values):
        sql = """INSERT INTO LivingSit(Value)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step4_create_livingsitid_to_livingsit_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_living="SELECT Value FROM LivingSit"
    livingsits = execute_sql_statement(sql_select_living, conn)
    livingsits=list(map(lambda row: row[0].strip(), livingsits))
    conn.close()
    our_dict={}
    count=1
    for i in livingsits:
        our_dict[i]=count
        count+=1
    return our_dict

def step5_education_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Education Status"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [Education] (
    [ID] Integer not null primary key,
    [Value] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "Education")


    def insert_values(conn, values):
        sql = """INSERT INTO Education(Value)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step5_create_Educationid_to_Eduation_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_education="SELECT Value FROM Education"
    edu = execute_sql_statement(sql_select_education, conn)
    edu=list(map(lambda row: row[0].strip(), edu))
    conn.close()
    our_dict={}
    count=1
    for i in edu:
        our_dict[i]=count
        count+=1
    return our_dict

def step6_household_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Household Composition"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [Household] (
    [HouseID] Integer not null primary key,
    [Composition] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "Household")


    def insert_values(conn, values):
        sql = """INSERT INTO Household(Composition)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step6_create_householdid_to_household_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_household="SELECT Composition FROM Household"
    households = execute_sql_statement(sql_select_household, conn)
    households=list(map(lambda row: row[0].strip(), households))
    conn.close()
    our_dict={}
    count=1
    for i in households:
        our_dict[i]=count
        count+=1
    return our_dict

def step7_employment_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Employment Status"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [Employment] (
    [ID] Integer not null primary key,
    [Status] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "Employment")


    def insert_values(conn, values):
        sql = """INSERT INTO Employment(Status)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION
def step7_create_empid_to_emp_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_employment="SELECT Status FROM Employment"
    empstatus = execute_sql_statement(sql_select_employment, conn)
    empstatus=list(map(lambda row: row[0].strip(), empstatus))
    conn.close()
    our_dict={}
    count=1
    for i in empstatus:
        our_dict[i]=count
        count+=1
    return our_dict

def step8_mental_illness_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    #getting data from data file and storing in data_dic
    data_dic = create_data_dic(data_filename)
    
    #taking out values from data_dic 
    unique_list = []
    for line in data_dic["Mental Illness"]:
        if line not in unique_list:
            unique_list.append(line)
    unique_list.sort()
        
        
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [MentalIllness] (
    [IllnessID] Integer not null primary key,
    [IllnessStatus] Text not null
    );
    """
    create_table(conn_norm, create_table_sql, "MentalIllness")


    def insert_values(conn, values):
        sql = """INSERT INTO MentalIllness(IllnessStatus)
                    VALUES(?)"""
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid


    with conn_norm:
        for count in unique_list:
            insert_values(conn_norm, (count,))

    ### END SOLUTION

def step8_create_illnessid_to_illness_dictionary(normalized_database_filename):
    conn = sqlite3.connect(normalized_database_filename)
    sql_select_Illness="SELECT IllnessStatus FROM MentalIllness"
    illness = execute_sql_statement(sql_select_Illness, conn)
    illness=list(map(lambda row: row[0].strip(), illness))
    conn.close()
    our_dict={}
    count=1
    for i in illness:
        our_dict[i]=count
        count+=1
    return our_dict

def step_9_patient_table(data_filename, normalized_database_filename):
    data_dic = create_data_dic(data_filename)
    programs=step1_create_program_to_programid_dictionary(normalized_database_filename)
    regions=step2_create_region_to_regionid_dictionary(normalized_database_filename)
    sexvalues=step3_create_sex_to_sexid_dictionary(normalized_database_filename)
    livingsits=step4_create_livingsitid_to_livingsit_dictionary(normalized_database_filename)
    educations=step5_create_Educationid_to_Eduation_dictionary(normalized_database_filename)
    households=step6_create_householdid_to_household_dictionary(normalized_database_filename)
    empstatuses=step7_create_empid_to_emp_dictionary(normalized_database_filename)
    illnesses=step8_create_illnessid_to_illness_dictionary(normalized_database_filename)
    patients = []
    for line in data_dic:
        index=line["index"]
        program=programs[line["Program Category"]]
        region=regions[line["Region Served"]]
        sex=sexvalues[line["Sex"]]
        livingsit=livingsits[line["Living Situation"]]
        edu=educations[line["Education Status"]]
        household=households[line["Household Composition"]]
        emp=empstatuses[line["Employment Status"]]
        illness=illnesses[line["Mental Illness"]]
        insert_tuple=(index,program,region,sex,livingsit,edu,household,emp,illness)
        patients.append(insert_tuple)
    #creating DB connection
    conn_norm = create_connection(normalized_database_filename)    
    
    #creating table schema in the db
    create_table_sql = """CREATE TABLE [Patients] (
    [PatientID] Integer not null primary key,
    [ProgramCategory] Integer not null,
    [RegionServed] Integer not null,
    [Sex] Integer not null,
    [LivingSituation] Integer not null,
    [EducationStatus] Integer not null,
    [HouseholdComposition] Integer not null,
    [EmploymentStatus] Integer not null,
    [MentalIllness] Integer not null,
    FOREIGN KEY(ProgramCategory) REFERENCES ProgramCategory(ProgramID)
    FOREIGN KEY(RegionServed) REFERENCES RegionsServed(RegionID)
    FOREIGN KEY(Sex) REFERENCES SexTable(SexID)
    FOREIGN KEY(LivingSituation) REFERENCES LivingSit(ID)
    FOREIGN KEY(EducationStatus) REFERENCES Education(ID)
    FOREIGN KEY(HouseholdComposition) REFERENCES Household(HouseID)
    FOREIGN KEY(EmploymentStatus) REFERENCES Employment(ID)
    FOREIGN KEY(MentalIllness) REFERENCES MentalIllness(IllnessID)
    );
    """
    create_table(conn_norm, create_table_sql, "Patients")

    sql_insert_patient = '''INSERT INTO Patients (PatientID,ProgramCategory,RegionServed,Sex,LivingSituation,EducationStatus,HouseholdComposition,EmploymentStatus,MentalIllness)
            VALUES(?,?,?,?,?,?,?,?,?) '''
    cur = conn_norm.cursor()
    cur.executemany(sql_insert_patient,patients)
    conn_norm.commit()
    sql_select_patients = ''' Select * from Patients'''
    patients_print = execute_sql_statement(sql_select_patients, conn_norm)
    conn_norm.close()