
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package com.uttarabankbd.com.datamigrationababil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *
 * @author jobayer.hossain
 */
public class DataMigrationAbabil {
private static final Logger LOGGER = LogManager.getLogger(DataMigrationAbabil.class);
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {
        // TODO code application logic here
        readOutward();
        
    }
    private  static void readOutward() throws SQLException{
        AbabilConnector ababilConn=new AbabilConnector();
        ChequemateConnector chkmateConn=new ChequemateConnector();
        try{
            
            Connection ababilCon=ababilConn.start();
            Connection chkmateCon=chkmateConn.start();
            if(ababilCon!=null && chkmateCon!=null){
                MigrateOutwardData(ababilCon,chkmateCon);
            }
        }catch(Exception e){
        LOGGER.error("Error Occured"+e.getStackTrace());
        e.printStackTrace();
        }finally{
        ababilConn.stop();
        chkmateConn.stop();
        }
    }private static void MigrateOutwardData(Connection ababilCon,Connection chkmateCon) throws FileNotFoundException, IOException{
        String appPropertiesPath = "DataMigrationAbabil.properties";

        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appPropertiesPath));
        String outwardFilePath=appProps.getProperty("OutwardFilePath");
        String sql="";
    }
    
}
