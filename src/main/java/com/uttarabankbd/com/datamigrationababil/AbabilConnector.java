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



/**
 *
 * @author jobayer.hossain
 */
public class AbabilConnector {
private static final Logger LOGGER = LogManager.getLogger(AbabilConnector.class);


    private static Connection con;
    public static String Driver="oracle.jdbc.driver.OracleDriver";
    
public AbabilConnector(){
try{
String ababildb="jdbc:oracle:thin:bach2/bach2@//172.18.100.12:1521/cpsdb";


    Class.forName(Driver);
    con=(Connection) DriverManager.getConnection(ababildb);
if(con!=null){
LOGGER.info("Ababil Database Connection Established Successfully");
}else{
LOGGER.error("Ababil Database Connection Failed");
}
}catch(Exception e){
LOGGER.error("Ababil Database Connection Failed "+e.getStackTrace());
e.printStackTrace();
}
}
public Connection start() throws SQLException{
      return con;      
    }public void stop() throws SQLException{
    con.close();
    }
}
