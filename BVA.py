import sys      #sys module for getting the argumnets
import pyodbc   #pyodbc module for connect python code to SQL server

bingo=0 #bingo is the variable to know if global has to be executed or not
 
#establishing the connection between SQL server and python script
try: 
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" "Server=SANKALP;""Database=maintenance;""Trusted_Connection=yes;")
    cursor = cnxn.cursor()
except:
    #error occured while connecting to database
    exit(2)
    ;

#insertion into output table
def insert_func(v_id,p,sub):
    #substuting the 'sub' at the place of @ symbol for matching the datatype
    try:
        a='insert into bi_env_variable_output (var_id,@) values(?,?)'
        b=a.replace('@',sub)
        cursor.execute(b,v_id,p)
        global bingo
        bingo=1 #insertion happended do one for global
        cursor.commit()
    except:
        exit(2)

#determining datatype and initation of insertion operation  
def ex_out(qw_to_out,v_id,da_ty):
    try:
        #getting the result
        cursor.execute(qw_to_out)
    except:
        #exception occured while executing the formula.
        exit(2)     
    #extracting the result of the formula
    try:
        for row in cursor:
            p=row[0]
    except:
        exit(2)
    #putting the result to corresponding column(datatype)
    if da_ty=='CHAR':
        #try inserting values to output table
        try:
            insert_func(v_id,p,'param_out_char')
        except:
            #error occured while insertion
            exit(2)
    elif da_ty=='NUM':
        try:
            #try inserting values to output table
            insert_func(v_id,p,'param_out_num')
        except:
            #error occured while insertion
            exit(2)
    else:
        try:
            #try inserting values to output table
            insert_func(v_id,p,"param_out_date")
        except:
            #error occured while insertion
            exit(2)
    
#begining of insertion operation
def func(var_int):
    s=list()
    try:
        val3=cursor.execute("select static_ind,var_id,formula,output_type,param_in_1_char,param_in_2_char,param_in_3_char,param_in_1_num,param_in_2_num,param_in_3_num,param_in_1_date,param_in_2_date,param_in_3_date from bi_env_variables where var_id=?",var_int).fetchall()
    except:
        exit(2)
    for row in val3:
        da_ty=str(row.output_type)
        v_id=row.var_id
        qw_to_out=row.formula
        val4=row.static_ind
        if val4!=0:
            try:
                print("bingo")
                if da_ty=='CHAR':
                    insert_func(v_id,qw_to_out,'param_out_char')
                elif da_ty=='DATE':
                    insert_func(v_id,qw_to_out,'param_out_date')
                else:
                    insert_func(v_id,qw_to_out,'param_out_num')
                return
            except:
                exit(2)
        pa_1_ch=row.param_in_1_char
        s.append(str(pa_1_ch))
        pa_2_ch=row.param_in_2_char
        s.append(str(pa_2_ch))
        pa_3_ch=row.param_in_3_char
        s.append(str(pa_3_ch))
        pa_1_nu=row.param_in_1_num
        s.append(str(pa_1_nu))
        pa_2_nu=row.param_in_2_num
        s.append(str(pa_2_nu))
        pa_3_nu=row.param_in_3_num
        s.append(str(pa_3_nu))
        pa_1_dat=row.param_in_1_date
        s.append(str(pa_1_dat))
        pa_2_dat=row.param_in_2_date
        s.append(str(pa_2_dat))
        pa_3_dat=row.param_in_3_date
        s.append(str(pa_3_dat))
    #print(s)
    #checking if any one of the parameters are used or not
    no_re=0
    for i in s:
        if(i != 'None'):
            no_re=1

    #if all the parameters are null then directly execute the formulae
    if no_re==0:
        #try executing the formula
        print(qw_to_out)
        ex_out(qw_to_out,v_id,da_ty)
    else:
        try:
            #replacing the $ values with corresponding values
            a=qw_to_out.replace('$PARAM_IN_1_CHAR',str(pa_1_ch))
            b=a.replace('$PARAM_IN_2_CHAR',str(pa_2_ch))
            c=b.replace('$PARAM_IN_3_CHAR',str(pa_3_ch))
            d=c.replace('$PARAM_IN_1_NUM',str(pa_1_nu))
            e=d.replace('$PARAM_IN_2_NUM',str(pa_2_nu))
            f=e.replace('$PARAM_IN_3_NUM',str(pa_3_nu))
            h=f.replace('$PARAM_IN_1_DATE',str(pa_1_dat))
            i=h.replace('$PARAM_IN_2_DATE',str(pa_2_dat))
            j=i.replace('$PARAM_IN_3_DATE',str(pa_3_dat))
        except:
            #trouble while replacing the values
            exit(2)
        #print(j)
        ex_out(j,v_id,da_ty)
 
#parametrized input during execution of code
#checking for number parametrs
if(len(sys.argv)>1):
    #checking for exact number of parameters
    if (len(sys.argv))==3:
        first_arg = sys.argv[1] #first argumets storing
        second_arg = sys.argv[2]  #second argumets storing

        #checking if the first arg is varible or market space
        
        #first arg is variable
        if first_arg=='-v':
            bingo=0
            #checking second arg if it is a integer or an string
            flag=0
            try:
                #int conversion of string will throw an error 
                var_int=int(second_arg)
            except:
                #error occured mean input is string and it is a invalid input, set the flag.
                flag=1
            
            #if flag is set exit status 2
            if flag==1:
                exit(2)
            
            #else proper input for -v
            else:
                try:
                    val5=cursor.execute("Select bi_id from bi_environments where bi_inv_name != 'GLOBAL' and bi_id in (select bi_id from bi_env_variables where var_id=?)",var_int).fetchall()
                except:
                    exit(2)
                if not val5:
                    exit(2)
                func(var_int)
                if bingo==1:
                    try:
                        print("bingo")
                        global_var=cursor.execute("select var_id from bi_env_variables where bi_id=1").fetchall()
                        for row in global_var:
                            kl=row[0]
                            func(kl)
                    except:
                        exit(2)
                    
        #if the input is market name
        elif first_arg=='-m':
            bingo=0
            #checking if proper input is given to market name.
            if second_arg.isdigit():
                exit(2)
            else:
                try:
                    ag=second_arg.split(',')
                except:
                    exit(2)
                s=[]
                print(ag)
                for i in ag:
                    #getting the bi_id for the given market name
                    try:
                        get_var=cursor.execute("select a.var_id from bi_env_variables as a,bi_environments as b where b.bi_market_name = ? and b.bi_id!=1 and a.bi_id=b.bi_id",str(i)).fetchall()
                    except:
                        exit(2)
                    if not get_var:
                        continue
                    for row in get_var:
                        kl=row[0]
                        func(kl)
                if bingo==1:
                    try:
                        global_var=cursor.execute("select var_id from bi_env_variables where bi_id=1").fetchall()
                        for row in global_var:
                            kl=row[0]
                            func(kl)
                    except:
                        exit(2)
        else:
            exit(2)
    #invalid number of arguments
    else:
        exit(2)

else:
    bingo=0
    try:
        get_var=cursor.execute("select var_id from bi_env_variables WHERE bi_id in (SELECT bi_id FROM bi_environments WHERE bi_inv_name != 'GLOBAL' AND bi_market_name IN (SELECT market FROM map_market_leg WHERE legal_entity_grp_code IN (SELECT legal_entity_group_code FROM bi_processing_markets)))").fetchall()
        if not get_var:
            exit(0)
        for row in get_var:
            kl=row[0]
            func(kl)
    except:
        exit(2)
    if bingo==1:
        try:
            global_va=cursor.execute("select var_id from bi_env_variables where bi_id=1").fetchall()
            for row in global_va:
                kl=row[0]
                func(kl)
        except:
            exit(2)
