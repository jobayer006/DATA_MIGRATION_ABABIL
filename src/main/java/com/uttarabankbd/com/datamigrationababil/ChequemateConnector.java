/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.uttarabankbd.com.datamigrationababil;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import oracle.jdbc.ora;
/**
 *
 * @author jobayer.hossain
 */
public class ChequemateConnector {
    private static Connection con;
    public static String Driver="oracle.jdbc.driver.OracleDriver";

private static final Logger LOGGER = LogManager.getLogger(ChequemateConnector.class);
public ChequemateConnector(){
try{

String chequematedb="jdbc:oracle:thin:chequemate/chequemate@//10.200.227.14:1521/bachdbbkp";
    Class.forName(Driver);
    con=(Connection) DriverManager.getConnection(chequematedb);
    
if(con!=null){
LOGGER.info("ChequeMate DB Connection Established Successfully");
}else{
LOGGER.error("ChequeMate DB Connection Failed");
}
}catch(Exception e){
LOGGER.error("Chequemate DB Connection Failed "+e.getStackTrace());
e.printStackTrace();
}
}
public Connection start() throws SQLException{
      return con;      
    }public void stop() throws SQLException{
    con.close();
    }
}
