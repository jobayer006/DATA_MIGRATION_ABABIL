
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package com.uttarabankbd.com.datamigrationababil;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Vector;
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
        readInward();
        
    }
    private static void readInward() throws SQLException{
    AbabilConnector ababilConn=new AbabilConnector();
        ChequemateConnector chkmateConn=new ChequemateConnector();
        try{
            
            Connection ababilCon=ababilConn.start();
            ababilCon.setAutoCommit(false);
            Connection chkmateCon=chkmateConn.start();
            chkmateCon.setAutoCommit(false);
            if(ababilCon!=null && chkmateCon!=null){
                MigrateInwardData(ababilCon,chkmateCon);
            }
        }catch(Exception e){
        LOGGER.error("Error Occured"+e.getStackTrace());
        e.printStackTrace();
        }finally{
        ababilConn.stop();
        chkmateConn.stop();
        }
    }
    private static void MigrateInwardData(Connection ababilCon, Connection chkmateCon) throws FileNotFoundException, IOException, JSchException, SQLException{
    String appPropertiesPath = "DataMigrationAbabil.properties";


        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appPropertiesPath));
        String inwardFilePath=appProps.getProperty("inwardpath");
        String from_date=appProps.getProperty("from_date");
        String to_date=appProps.getProperty("to_date");
        String ababil_server=appProps.getProperty("ababil_server");
        String ababil_username=appProps.getProperty("ababil_username");
        String ababil_password=appProps.getProperty("ababil_password");
        String inward_status=appProps.getProperty("inward_status");
        int SFTPPort=22;
        ChannelSftp channelSftp=null;    
                  JSch jsch = new JSch();
    //jsch.setKnownHosts("../.ssh/known_hosts");
    Session jschSession = jsch.getSession(ababil_username, ababil_server, SFTPPort);
    jschSession.setPassword(ababil_password);
    jschSession.setConfig("StrictHostKeyChecking", "no");
    jschSession.connect(2000);
    jschSession.setConfig("kex", "diffie-hellman-group-exchange-sha256"); 
    channelSftp =(ChannelSftp) jschSession.openChannel("sftp");
    channelSftp.connect();
    LOGGER.info("FTP Connection with ABABIL Server Established Successfully");
    if((inward_status.toUpperCase()).equals("ON")){
        
    LOGGER.info("Migrating Inward Data from "+from_date+" to "+to_date);
    
    String query="select t.id                SL_NO,\n" +
"       t.branchcode        BRANCH_ID,\n" +
"       t.ecesettlementdate HOUSE_DT,\n" +
"       case when t.verifystatus in (0,1) then 2\n" +
"         when t.verifystatus in (2,3) then 3\n" +
"           else 0 end as TRANS_STATUS,\n" +
"       case when t.itemvalue_ecetype_id=1 then '19'\n" +
"         else '11' end as CLR_TYPE,\n" +
"       t.chequesequencenumber CHQ_NUMBER,\n" +
"       t.issuingbranchroutingnumber PAYEE_ROUTNO,\n" +
"       t.branchcode PAYEE_BR_ID,\n" +
"       substr(t.accountnumber,3,13) PAYEE_ACCNO,\n" +
"       'BDT' CURRENCY_CODE,\n" +
"       1.0000 EXCHANGE_RATE,\n" +
"       '' EXCHANGE_RATE_ID,\n" +
"       t.itemamount AMOUNT_CCY,\n" +
"       t.itemamount AMOUNT_LCY,\n" +
"       t.trancode TRANS_CODE,\n" +
"       f.returnlocationroutingnumber BENF_ROUTNO,\n" +
"       case when rr.code is null then 0 \n" +
"         else 1 end as RETURN_FLAG,\n" +
"       rr.code RETURN_ID,\n" +
"       'N' ECE_TYPE,\n" +
"       t.eceinstitutionitemseqnumber ITEMSEQNUM,\n" +
"       f.cashletterid CASHLETTER_ID,\n" +
"       f.eceinstitutionroutingnumber ECEINSTITUTE_RTNO,\n" +
"       f.returnlocationroutingnumber RETURN_BR_RT,\n" +
"       f.filename FILENAME,\n" +
"       t.representmentindicator PRESENTMENT,\n" +
"       t.accountnumber ACTUAL_PAYEE_ACCNO,\n" +
"       t.chequesequencenumber ACTUAL_CHQ_NUMBER,\n" +
"       'JAVAUSER' MAKE_BY,\n" +
"       t.ecesettlementtime MAKE_DT,\n" +
"       'JAVAUSER' AUTH_1ST_BY,\n" +
"       t.ecesettlementtime AUTH_1ST_DT,\n" +
"       'A' AUTH_STATUS_ID,\n" +
"       'MigrationData'|| t.id BUID,\n" +
"       0 REVERSE_FLAG,\n" +
"       dt.imagename FRONT_IMG_NAME,\n" +
"       --(select imagename from imagedetail dtt where dtt.viewsideindicator=1 and dtt.item_id=t.id)REAR_IMG_NAME,\n" +
"       t.itemcreationdate CREATION_DT\n" +
"  from iteminfo t\n" +
"  full join fileinfo f on t.fileinfo_id=f.id\n" +
"  full join return_reason rr on rr.id=t.returnreason_id\n" +
"  full join imagedetail dt on dt.item_id=t.id\n" +
" where t.Ecesettlementdate between  "+from_date+" and "+to_date +"\n" +
" and t.itemvalue_ecetype_id in (1,2)\n" +
" and t.is_migrated=0\n" +
" and dt.viewsideindicator=0 order by t.ecesettlementdate";
        
        try{
        PreparedStatement pst=ababilCon.prepareStatement(query);
        
        ResultSet rs=pst.executeQuery();
        while(rs.next()){
            String SL_NO=rs.getString("SL_NO");
       String BRANCH_ID=rs.getString("BRANCH_ID");
       Date HOUSE_DT=rs.getDate("HOUSE_DT");
       int TRANS_STATUS=rs.getInt("TRANS_STATUS");
       String CLR_TYPE=rs.getString("CLR_TYPE");
       String CHQ_NUMBER=rs.getString("CHQ_NUMBER");
       String PAYEE_ROUTNO=rs.getString("PAYEE_ROUTNO");
       String PAYEE_BR_ID=rs.getString("PAYEE_BR_ID");
       String PAYEE_ACCNO=rs.getString("PAYEE_ACCNO");
       String CURRENCY_CODE=rs.getString("CURRENCY_CODE");
       int EXCHANGE_RATE=rs.getInt("EXCHANGE_RATE");
       String EXCHANGE_RATE_ID="";
       double AMOUNT_CCY=rs.getDouble("AMOUNT_CCY");
       double AMOUNT_LCY=rs.getDouble("AMOUNT_LCY");
       String TRANS_CODE=rs.getString("TRANS_CODE");
       String BENF_ROUTNO=rs.getString("BENF_ROUTNO");
       int RETURN_FLAG=rs.getInt("RETURN_FLAG");
       String RETURN_ID=rs.getString("RETURN_ID");
       String ECE_TYPE=rs.getString("ECE_TYPE");
       int PRESENTMENT=rs.getInt("PRESENTMENT");
       String ITEMSEQNUM=rs.getString("ITEMSEQNUM");
       String CASHLETTER_ID=rs.getString("CASHLETTER_ID");
       String ECEINSTITUTE_RTNO=rs.getString("ECEINSTITUTE_RTNO");
       String RETURN_BR_RT=rs.getString("RETURN_BR_RT");
       String FILENAME=rs.getString("FILENAME");
       String ACTUAL_PAYEE_ACCNO=rs.getString("ACTUAL_PAYEE_ACCNO");
       String ACTUAL_CHQ_NUMBER=rs.getString("ACTUAL_CHQ_NUMBER");
       String MAKE_BY=rs.getString("MAKE_BY");
       Date MAKE_DT=rs.getDate("MAKE_DT");
       String AUTH_1ST_BY=rs.getString("AUTH_1ST_BY");
       Date AUTH_1ST_DT=rs.getDate("AUTH_1ST_DT");
       String FRONT_IMG_NAME=rs.getString("FRONT_IMG_NAME");//*******For file copy
       String frimgnm=FRONT_IMG_NAME.replace(".tif", "");
       String REAR_IMG_NAME="";
       String lastchar=frimgnm.substring(frimgnm.length()-2, frimgnm.length());
       System.out.println("Lastchar:"+lastchar);
       try{
       int lastnum=Integer.parseInt(lastchar);
       lastnum=lastnum+1;
       REAR_IMG_NAME=frimgnm.substring(0,frimgnm.length()-2)+lastnum+".tif";
       System.out.println();
       }catch(Exception e){
           lastchar=frimgnm.substring(frimgnm.length()-1, frimgnm.length());
           int num=Integer.parseInt(lastchar)+1;
       REAR_IMG_NAME=frimgnm.substring(0,frimgnm.length()-1)+num+".tif";
       
       }
       System.out.println(REAR_IMG_NAME);
       //String REAR_IMG_NAME=rs.getString("REAR_IMG_NAME");//*****For file copy
       Date SCAN_DT=rs.getDate("CREATION_DT");//******For file copy
       String AUTH_STATUS_ID=rs.getString("AUTH_STATUS_ID");
       String BUID=rs.getString("BUID");
       int REVERSE_FLAG=rs.getInt("REVERSE_FLAG");
       LOGGER.info(SL_NO+" : "+CHQ_NUMBER+" : "+AMOUNT_CCY+" : "+FRONT_IMG_NAME+" : "+HOUSE_DT+" : "+SCAN_DT);
       
       String frontImageLocation=inwardFilePath+SCAN_DT+"/image/"+FRONT_IMG_NAME;
       String rearImageLocation=inwardFilePath+SCAN_DT+"/image/"+REAR_IMG_NAME;
       if(!(FRONT_IMG_NAME==null)||!(REAR_IMG_NAME==null)){
       downloadFile(frontImageLocation,rearImageLocation,FRONT_IMG_NAME,REAR_IMG_NAME,channelSftp,false);
       String localfrontimage="D:\\TEMP\\"+FRONT_IMG_NAME;
        String localrearimage="D:\\TEMP\\"+REAR_IMG_NAME;
        FileInputStream frontis=new FileInputStream(new File(localfrontimage));
        FileInputStream rearis=new FileInputStream(new File(localrearimage));
        //System.out.println(HOUSE_DT+","+MAKE_DT+","+AUTH_1ST_DT+","+AUTH_2ND_DT);
       //System.out.println(frontImageLocation);
       //System.out.println(rearImageLocation);
       String query2="INSERT INTO BACH_INWARD_REG_HIST (SL_NO,BRANCH_ID,HOUSE_DT,TRANS_STATUS,CLR_TYPE,"
               + "CHQ_NUMBER,PAYEE_ROUTNO,PAYEE_BR_ID,PAYEE_ACCNO,CURRENCY_CODE,EXCHANGE_RATE,EXCHANGE_RATE_ID,"
               + "AMOUNT_CCY,AMOUNT_LCY,TRANS_CODE,BENF_ROUTNO,RETURN_FLAG,"
               + "RETURN_ID,ECE_TYPE,ITEMSEQNUM,CASHLETTER_ID,ECEINSTITUTE_RTNO,RETURN_BR_RT,FILENAME,PRESENTMENT,"
               + "ACTUAL_PAYEE_ACCNO,ACTUAL_CHQ_NUMBER,MAKE_BY,MAKE_DT,AUTH_1ST_BY,AUTH_1ST_DT,"
               + "AUTH_STATUS_ID,BUID,REVERSE_FLAG) VALUES "
               + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
               
       PreparedStatement pst2=chkmateCon.prepareStatement(query2);
       
       	   pst2.setString(1,SL_NO);
       pst2.setString(2,BRANCH_ID);
       pst2.setDate(3,HOUSE_DT);
       pst2.setInt(4,TRANS_STATUS);
       pst2.setString(5,CLR_TYPE);
       pst2.setString(6,CHQ_NUMBER);
       pst2.setString(7,PAYEE_ROUTNO);
       pst2.setString(8,PAYEE_BR_ID);
       pst2.setString(9,PAYEE_ACCNO);
       pst2.setString(10,CURRENCY_CODE);
       pst2.setInt(11,EXCHANGE_RATE);
       pst2.setString(12,EXCHANGE_RATE_ID);
       pst2.setDouble(13,AMOUNT_CCY);
       pst2.setDouble(14,AMOUNT_LCY);
       pst2.setString(15,TRANS_CODE);
       pst2.setString(16,BENF_ROUTNO);
       pst2.setInt(17,RETURN_FLAG);
       pst2.setString(18,RETURN_ID);
       pst2.setString(19,ECE_TYPE);
       pst2.setString(20,ITEMSEQNUM);
       pst2.setString(21,CASHLETTER_ID);
       pst2.setString(22,ECEINSTITUTE_RTNO);
       pst2.setString(23,RETURN_BR_RT);
       pst2.setString(24,FILENAME);
       pst2.setInt(25,PRESENTMENT);
       pst2.setString(26, ACTUAL_PAYEE_ACCNO);
       pst2.setString(27, ACTUAL_CHQ_NUMBER);
       pst2.setString(28, MAKE_BY);
       if(MAKE_DT==null){
       pst2.setDate(29,HOUSE_DT);
       }else{
       pst2.setDate(29,MAKE_DT);
       }
       pst2.setString(30, AUTH_1ST_BY);
       pst2.setDate(31, AUTH_1ST_DT);
       pst2.setString(32, AUTH_STATUS_ID);
       pst2.setString(33, BUID);
       pst2.setInt(34, REVERSE_FLAG);
       
       
       pst2.execute();
       pst2.close();
       
       
       
       String query3="INSERT INTO BACH_INWARD_IMG_HIST (BRANCH_ID,HOUSE_DT,IMAGE_FRONT,IMAGE_REAR,SL_NO_MAST,BUID)"
               + " VALUES(?,?,?,?,?,?)";
       PreparedStatement pst3=chkmateCon.prepareStatement(query3);
       pst3.setString(1, BRANCH_ID);
       pst3.setDate(2, HOUSE_DT);
       pst3.setBinaryStream(3,frontis );
       pst3.setBinaryStream(4, rearis);
       pst3.setString(5, SL_NO);
       pst3.setString(6, BUID);
       pst3.execute();
       pst3.close();
       
       String query4="UPDATE ITEMINFO I SET I.IS_MIGRATED=1 WHERE I.ID=?";
       PreparedStatement pst4=ababilCon.prepareStatement(query4);
       pst4.setString(1, SL_NO);
       pst4.execute();
       pst4.close();
       LOGGER.info("DATA Inserted Successfully");
       ababilCon.commit();
       chkmateCon.commit();
       File localfrontimageF=new File(localfrontimage);
       File localrearimageF=new File(localrearimage);
       frontis.close();
       rearis.close();
       localfrontimageF.delete();
       localrearimageF.delete();
       LOGGER.info("File: "+localfrontimage+ " Deleted Successfully");
       }else{
        
        //System.out.println(HOUSE_DT+","+MAKE_DT+","+AUTH_1ST_DT+","+AUTH_2ND_DT);
       //System.out.println(frontImageLocation);
       //System.out.println(rearImageLocation);
       String query2="INSERT INTO BACH_INWARD_REG_HIST (SL_NO,BRANCH_ID,HOUSE_DT,TRANS_STATUS,CLR_TYPE,"
               + "CHQ_NUMBER,PAYEE_ROUTNO,PAYEE_BR_ID,PAYEE_ACCNO,CURRENCY_CODE,EXCHANGE_RATE,EXCHANGE_RATE_ID,"
               + "AMOUNT_CCY,AMOUNT_LCY,TRANS_CODE,BENF_ROUTNO,RETURN_FLAG,"
               + "RETURN_ID,ECE_TYPE,ITEMSEQNUM,CASHLETTER_ID,ECEINSTITUTE_RTNO,RETURN_BR_RT,FILENAME,PRESENTMENT,"
               + "ACTUAL_PAYEE_ACCNO,ACTUAL_CHQ_NUMBER,MAKE_BY,MAKE_DT,AUTH_1ST_BY,AUTH_1ST_DT,"
               + "AUTH_STATUS_ID,BUID,REVERSE_FLAG) VALUES "
               + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
               
       PreparedStatement pst2=chkmateCon.prepareStatement(query2);
       
       	   pst2.setString(1,SL_NO);
       pst2.setString(2,BRANCH_ID);
       pst2.setDate(3,HOUSE_DT);
       pst2.setInt(4,TRANS_STATUS);
       pst2.setString(5,CLR_TYPE);
       pst2.setString(6,CHQ_NUMBER);
       pst2.setString(7,PAYEE_ROUTNO);
       pst2.setString(8,PAYEE_BR_ID);
       pst2.setString(9,PAYEE_ACCNO);
       pst2.setString(10,CURRENCY_CODE);
       pst2.setInt(11,EXCHANGE_RATE);
       pst2.setString(12,EXCHANGE_RATE_ID);
       pst2.setDouble(13,AMOUNT_CCY);
       pst2.setDouble(14,AMOUNT_LCY);
       pst2.setString(15,TRANS_CODE);
       pst2.setString(16,BENF_ROUTNO);
       pst2.setInt(17,RETURN_FLAG);
       pst2.setString(18,RETURN_ID);
       pst2.setString(19,ECE_TYPE);
       pst2.setString(20,ITEMSEQNUM);
       pst2.setString(21,CASHLETTER_ID);
       pst2.setString(22,ECEINSTITUTE_RTNO);
       pst2.setString(23,RETURN_BR_RT);
       pst2.setString(24,FILENAME);
       pst2.setInt(25,PRESENTMENT);
       pst2.setString(26, ACTUAL_PAYEE_ACCNO);
       pst2.setString(27, ACTUAL_CHQ_NUMBER);
       pst2.setString(28, MAKE_BY);
       if(MAKE_DT==null){
       pst2.setDate(29,HOUSE_DT);
       }else{
       pst2.setDate(29,MAKE_DT);
       }
       pst2.setString(30, AUTH_1ST_BY);
       pst2.setDate(31, AUTH_1ST_DT);
       pst2.setString(32, AUTH_STATUS_ID);
       pst2.setString(33, BUID);
       pst2.setInt(34, REVERSE_FLAG);
       pst2.execute();
       pst2.close();
       
       
       
       String query3="INSERT INTO BACH_INWARD_IMG_HIST (BRANCH_ID,HOUSE_DT,IMAGE_FRONT,IMAGE_REAR,SL_NO_MAST,BUID)"
               + " VALUES(?,?,?,?,?,?)";
       PreparedStatement pst3=chkmateCon.prepareStatement(query3);
       pst3.setString(1, BRANCH_ID);
       pst3.setDate(2, HOUSE_DT);
       pst3.setBinaryStream(3,null );
       pst3.setBinaryStream(4, null);
       pst3.setString(5, SL_NO);
       pst3.setString(6, BUID);
       pst3.execute();
       pst3.close();
       
       String query4="UPDATE ITEMINFO I SET I.IS_MIGRATED=1 WHERE I.ID=?";
       PreparedStatement pst4=ababilCon.prepareStatement(query4);
       pst4.setString(1, SL_NO);
       pst4.execute();
       pst4.close();
       LOGGER.info("DATA Inserted Successfully");
       ababilCon.commit();
       chkmateCon.commit();
       } 
       }
        }catch(Exception e){
            ababilCon.rollback();
            chkmateCon.rollback();
        LOGGER.error("ERROR OCCURED IN EXECUTING QUERY");
        e.printStackTrace();
        }finally{
        channelSftp.exit();
LOGGER.info("FTP Connection ended");
        }
    }else{
    LOGGER.info("Inward Data Migration Status is set to OFF");
    }
    }
    private  static void readOutward() throws SQLException{
        AbabilConnector ababilConn=new AbabilConnector();
        ChequemateConnector chkmateConn=new ChequemateConnector();
        try{
            
            Connection ababilCon=ababilConn.start();
            ababilCon.setAutoCommit(false);
            Connection chkmateCon=chkmateConn.start();
            chkmateCon.setAutoCommit(false);
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
    }private static void MigrateOutwardData(Connection ababilCon,Connection chkmateCon) throws FileNotFoundException, IOException, ParseException, SQLException, JSchException{
        String appPropertiesPath = "DataMigrationAbabil.properties";


        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appPropertiesPath));
        String outwardFilePath=appProps.getProperty("outwardpath");
        String from_date=appProps.getProperty("from_date");
        String to_date=appProps.getProperty("to_date");
        String ababil_server=appProps.getProperty("ababil_server");
        String ababil_username=appProps.getProperty("ababil_username");
        String ababil_password=appProps.getProperty("ababil_password");
        String outward_status=appProps.getProperty("outward_status");
        
        int SFTPPort=22;
        ChannelSftp channelSftp=null;    
                  JSch jsch = new JSch();
    //jsch.setKnownHosts("../.ssh/known_hosts");
    Session jschSession = jsch.getSession(ababil_username, ababil_server, SFTPPort);
    jschSession.setPassword(ababil_password);
    jschSession.setConfig("StrictHostKeyChecking", "no");
    jschSession.connect(2000);
    jschSession.setConfig("kex", "diffie-hellman-group-exchange-sha256"); 
    channelSftp =(ChannelSftp) jschSession.openChannel("sftp");
    channelSftp.connect();
    LOGGER.info("FTP Connection with ABABIL Server Established Successfully");
    if((outward_status.toUpperCase()).equals("ON")){
        
        LOGGER.info("Migrating Outward Data from "+from_date+" to "+to_date);
        //System.out.println("TO_DATE: "+to_date);
                
        String query="select t.id SL_NO,\n" +
"       b.branchcode BRANCH_ID,\n" +
"       t.housedate HOUSE_DT,\n" +
"       case when t.outwarditemstatus in (0,1,2) then 4\n" +
"         else  7 end as TRANS_STATUS,\n" +
"       case when t.itemvalue_ecetype_id=4 then '09'\n" +
"         else '01' end as CLR_TYPE,\n" +
"       t.chequesequencenumber CHQ_NUMBER,\n" +
"       t.issuingbranchroutingnumber PAYEE_ROUTNO,\n" +
"       t.accountnumber PAYEE_ACCNO,\n" +
"       t.presentingbank PAYEE_BANKNM,\n" +
"       'BDT' as CURRENCY_CODE,\n" +
"       1 as EXCHANGE_RATE,\n" +
"       t.itemamount AMOUNT_CCY,\n" +
"       t.itemamount AMOUNT_LCY,\n" +
"       t.housedate-1 as CHQ_DT,\n" +
"       t.trancode TRANS_CODE,\n" +
"       concat(b.routing_number,b.check_digit) ORGN_ROUTNO,\n" +
"       b2.branchcode BENF_BRANCHID,\n" +
"       case when t.returnreason_id is null then 0\n" +
"         else 1 end as BOUNCE_FLAG,\n" +
"       rr.code RETURN_ID,\n" +
"       t.representmentindicator PRESENTMENT,\n" +
"       t.eceinstitutionitemseqnumber ITEMSEQNUM,\n" +
"       f.filename FILENAME,\n" +
"       t.issuingbranchroutingnumber PAYEEBANKRT,\n" +
"       concat(b.routing_number,b.check_digit) BENFBANKRT,\n" +
"       t.remarks PBM_REJECT_REASON,\n" +
"       m.entrytime MAKE_DT,\n" +
"       t.itemtrantime AUTH_1ST_DT,\n" +
"       t.itemtrantime AUTH_2ND_DT,\n" +
"       m.fontimagename FRONT_IMG_NAME,\n" +
"       m.rearimagename REAR_IMG_NAME,\n" +
"       m.housedate SCAN_DT\n" +
"  from iteminfo t\n" +
"  full join makerinfo m on m.id=t.makerinfoid\n" +
"  full join bank_branches b on b.id=m.scanningbranch_id \n" +
"  full join bank_branches b2 on b2.id=m.recipientbranch_id\n" +
"  inner join fileinfo f on f.id=t.fileinfo_id\n" +
"  full join return_reason rr on rr.id=t.returnreason_id\n" +
" where t.itemvalue_ecetype_id in (3, 4)\n" +
" and t.housedate between "+from_date+" and "+to_date +"\n" +
" and t.is_migrated=0";
        //System.out.println(query);
        
        try{
        PreparedStatement pst=ababilCon.prepareStatement(query);
        ResultSet rs=pst.executeQuery();
        while(rs.next()){
            String SL_NO=rs.getString("SL_NO");
       String BRANCH_ID=rs.getString("BRANCH_ID");
       Date HOUSE_DT=rs.getDate("HOUSE_DT");
       int TRANS_STATUS=rs.getInt("TRANS_STATUS");
       String CLR_TYPE=rs.getString("CLR_TYPE");
       String CHQ_NUMBER=rs.getString("CHQ_NUMBER");
       String PAYEE_ROUTNO=rs.getString("PAYEE_ROUTNO");
       String PAYEE_ACCNO=rs.getString("PAYEE_ACCNO");
       String PAYEE_BANKNM=rs.getString("PAYEE_BANKNM");
       String CURRENCY_CODE=rs.getString("CURRENCY_CODE");
       int EXCHANGE_RATE=rs.getInt("EXCHANGE_RATE");
       double AMOUNT_CCY=rs.getDouble("AMOUNT_CCY");
       double AMOUNT_LCY=rs.getDouble("AMOUNT_LCY");
       Date CHQ_DT=rs.getDate("CHQ_DT");
       String TRANS_CODE=rs.getString("TRANS_CODE");
       String ORGN_ROUTNO=rs.getString("ORGN_ROUTNO");
       String BENF_BRANCHID=rs.getString("BENF_BRANCHID");
       int BOUNCE_FLAG=rs.getInt("BOUNCE_FLAG");
       String RETURN_ID=rs.getString("RETURN_ID");
       int PRESENTMENT=rs.getInt("PRESENTMENT");
       String ITEMSEQNUM=rs.getString("ITEMSEQNUM");
       String FILENAME=rs.getString("FILENAME");
       String  PAYEEBANKRT=rs.getString("PAYEEBANKRT");
       String BENFBANKRT=rs.getString("BENFBANKRT");
       String PBM_REJECT_REASON=rs.getString("PBM_REJECT_REASON");
       Date MAKE_DT=rs.getDate("MAKE_DT");
       Date AUTH_1ST_DT=rs.getDate("AUTH_1ST_DT");
       Date AUTH_2ND_DT=rs.getDate("AUTH_2ND_DT");
       String FRONT_IMG_NAME=rs.getString("FRONT_IMG_NAME");
       String REAR_IMG_NAME=rs.getString("REAR_IMG_NAME");
       Date SCAN_DT=rs.getDate("SCAN_DT");
       LOGGER.info(SL_NO+" : "+CHQ_NUMBER+" : "+AMOUNT_CCY+" : "+FRONT_IMG_NAME+" : "+HOUSE_DT+" : "+SCAN_DT);
       
       String frontImageLocation=outwardFilePath+SCAN_DT+"/"+FRONT_IMG_NAME;
       String rearImageLocation=outwardFilePath+SCAN_DT+"/"+REAR_IMG_NAME;
       if(!(FRONT_IMG_NAME==null)||!(REAR_IMG_NAME==null)){
       downloadFile(frontImageLocation,rearImageLocation,FRONT_IMG_NAME,REAR_IMG_NAME,channelSftp,true);
       String localfrontimage="D:\\TEMP\\"+FRONT_IMG_NAME;
        String localrearimage="D:\\TEMP\\"+REAR_IMG_NAME;
        FileInputStream frontis=new FileInputStream(new File(localfrontimage));
        FileInputStream rearis=new FileInputStream(new File(localrearimage));
        //System.out.println(HOUSE_DT+","+MAKE_DT+","+AUTH_1ST_DT+","+AUTH_2ND_DT);
       //System.out.println(frontImageLocation);
       //System.out.println(rearImageLocation);
       String query2="INSERT INTO BACH_OUTWARD_REG_HIST (SL_NO,BRANCH_ID,HOUSE_DT,TRANS_STATUS,CLR_TYPE,"
               + "CHQ_NUMBER,PAYEE_ROUTNO,PAYEE_ACCNO,PAYEE_BANKNM,CURRENCY_CODE,EXCHANGE_RATE,"
               + "AMOUNT_CCY,AMOUNT_LCY,CHQ_DT,TRANS_CODE,ORGN_ROUTNO,BENF_BRANCHID,BOUNCE_FLAG,"
               + "RETURN_ID,PRESENTMENT,ITEMSEQNUM,FILENAME,PAYEEBANKRT,BENFBANKRT,PBM_REJECT_REASON,"
               + "MAKE_DT,AUTH_1ST_DT,AUTH_2ND_DT,MAKE_BY,BUID,REVERSE_FLAG,CHARGEFLAG,AUTH_STATUS_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
               
       PreparedStatement pst2=chkmateCon.prepareStatement(query2);
       
       	   pst2.setString(1,SL_NO);
       pst2.setString(2,BRANCH_ID);
       pst2.setDate(3,HOUSE_DT);
       pst2.setInt(4,TRANS_STATUS);
       pst2.setString(5,CLR_TYPE);
       pst2.setString(6,CHQ_NUMBER);
       pst2.setString(7,PAYEE_ROUTNO);
       pst2.setString(8,PAYEE_ACCNO);
       pst2.setString(9,PAYEE_BANKNM);
       pst2.setString(10,CURRENCY_CODE);
       pst2.setInt(11,EXCHANGE_RATE);
       pst2.setDouble(12,AMOUNT_CCY);
       pst2.setDouble(13,AMOUNT_LCY);
       pst2.setDate(14,CHQ_DT);
       pst2.setString(15,TRANS_CODE);
       pst2.setString(16,ORGN_ROUTNO);
       pst2.setString(17,BENF_BRANCHID);
       pst2.setInt(18,BOUNCE_FLAG);
       pst2.setString(19,RETURN_ID);
       pst2.setInt(20,PRESENTMENT);
       pst2.setString(21,ITEMSEQNUM);
       pst2.setString(22,FILENAME);
       pst2.setString(23,PAYEEBANKRT);
       pst2.setString(24,BENFBANKRT);
       pst2.setString(25,PBM_REJECT_REASON);
       if(MAKE_DT==null){
       pst2.setDate(26,HOUSE_DT);
       }else{
       pst2.setDate(26,MAKE_DT);
       }
       pst2.setDate(27,AUTH_1ST_DT);
       pst2.setDate(28,AUTH_2ND_DT);
       pst2.setString(29,"JAVAUSER");
       pst2.setString(30, "Migrated:"+SL_NO);
       pst2.setInt(31, 0);
       pst2.setInt(32, 1);
       pst2.setString(33, "A");
       pst2.execute();
       pst2.close();
       
       
       
       String query3="INSERT INTO BACH_OUTWARD_IMG_HIST (BRANCH_ID,HOUSE_DT,IMAGE_FRONT,IMAGE_REAR,SL_NO_MAST,BUID)"
               + " VALUES(?,?,?,?,?,?)";
       PreparedStatement pst3=chkmateCon.prepareStatement(query3);
       pst3.setString(1, BRANCH_ID);
       pst3.setDate(2, HOUSE_DT);
       pst3.setBinaryStream(3,frontis );
       pst3.setBinaryStream(4, rearis);
       pst3.setString(5, SL_NO);
       pst3.setString(6, "Migrated"+SL_NO);
       pst3.execute();
       pst3.close();
       
       String query4="UPDATE ITEMINFO I SET I.IS_MIGRATED=1 WHERE I.ID=?";
       PreparedStatement pst4=ababilCon.prepareStatement(query4);
       pst4.setString(1, SL_NO);
       pst4.execute();
       pst4.close();
       LOGGER.info("DATA Inserted Successfully");
       ababilCon.commit();
       chkmateCon.commit();
       File localfrontimageF=new File(localfrontimage);
       File localrearimageF=new File(localrearimage);
       frontis.close();
       rearis.close();
       localfrontimageF.delete();
       localrearimageF.delete();
       LOGGER.info("File: "+localfrontimage+ " Deleted Successfully");
       }else{
        
        //System.out.println(HOUSE_DT+","+MAKE_DT+","+AUTH_1ST_DT+","+AUTH_2ND_DT);
       //System.out.println(frontImageLocation);
       //System.out.println(rearImageLocation);
       String query2="INSERT INTO BACH_OUTWARD_REG_HIST (SL_NO,BRANCH_ID,HOUSE_DT,TRANS_STATUS,CLR_TYPE,"
               + "CHQ_NUMBER,PAYEE_ROUTNO,PAYEE_ACCNO,PAYEE_BANKNM,CURRENCY_CODE,EXCHANGE_RATE,"
               + "AMOUNT_CCY,AMOUNT_LCY,CHQ_DT,TRANS_CODE,ORGN_ROUTNO,BENF_BRANCHID,BOUNCE_FLAG,"
               + "RETURN_ID,PRESENTMENT,ITEMSEQNUM,FILENAME,PAYEEBANKRT,BENFBANKRT,PBM_REJECT_REASON,"
               + "MAKE_DT,AUTH_1ST_DT,AUTH_2ND_DT,MAKE_BY,BUID,REVERSE_FLAG,CHARGEFLAG,AUTH_STATUS_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
               
       PreparedStatement pst2=chkmateCon.prepareStatement(query2);
       
       	   pst2.setString(1,SL_NO);
       pst2.setString(2,BRANCH_ID);
       pst2.setDate(3,HOUSE_DT);
       pst2.setInt(4,TRANS_STATUS);
       pst2.setString(5,CLR_TYPE);
       pst2.setString(6,CHQ_NUMBER);
       pst2.setString(7,PAYEE_ROUTNO);
       pst2.setString(8,PAYEE_ACCNO);
       pst2.setString(9,PAYEE_BANKNM);
       pst2.setString(10,CURRENCY_CODE);
       pst2.setInt(11,EXCHANGE_RATE);
       pst2.setDouble(12,AMOUNT_CCY);
       pst2.setDouble(13,AMOUNT_LCY);
       pst2.setDate(14,CHQ_DT);
       pst2.setString(15,TRANS_CODE);
       pst2.setString(16,ORGN_ROUTNO);
       pst2.setString(17,BENF_BRANCHID);
       pst2.setInt(18,BOUNCE_FLAG);
       pst2.setString(19,RETURN_ID);
       pst2.setInt(20,PRESENTMENT);
       pst2.setString(21,ITEMSEQNUM);
       pst2.setString(22,FILENAME);
       pst2.setString(23,PAYEEBANKRT);
       pst2.setString(24,BENFBANKRT);
       pst2.setString(25,PBM_REJECT_REASON);
       if(MAKE_DT==null){
       pst2.setDate(26,HOUSE_DT);
       }else{
       pst2.setDate(26,MAKE_DT);
       }
       pst2.setDate(27,AUTH_1ST_DT);
       pst2.setDate(28,AUTH_2ND_DT);
       pst2.setString(29,"JAVAUSER");
       pst2.setString(30, "Migrated:"+SL_NO);
       pst2.setInt(31, 0);
       pst2.setInt(32, 1);
       pst2.setString(33, "A");
       pst2.execute();
       pst2.close();
       
       
       
       String query3="INSERT INTO BACH_OUTWARD_IMG_HIST (BRANCH_ID,HOUSE_DT,IMAGE_FRONT,IMAGE_REAR,SL_NO_MAST,BUID)"
               + " VALUES(?,?,?,?,?,?)";
       PreparedStatement pst3=chkmateCon.prepareStatement(query3);
       pst3.setString(1, BRANCH_ID);
       pst3.setDate(2, HOUSE_DT);
       pst3.setBinaryStream(3,null );
       pst3.setBinaryStream(4, null);
       pst3.setString(5, SL_NO);
       pst3.setString(6, "Migrated"+SL_NO);
       pst3.execute();
       pst3.close();
       
       String query4="UPDATE ITEMINFO I SET I.IS_MIGRATED=1 WHERE I.ID=?";
       PreparedStatement pst4=ababilCon.prepareStatement(query4);
       pst4.setString(1, SL_NO);
       pst4.execute();
       pst4.close();
       LOGGER.info("DATA Inserted Successfully");
       ababilCon.commit();
       chkmateCon.commit();
       } 
       }
        }catch(Exception e){
            ababilCon.rollback();
            chkmateCon.rollback();
        LOGGER.error("ERROR OCCURED IN EXECUTING QUERY");
        e.printStackTrace();
        }finally{
        channelSftp.exit();
LOGGER.info("FTP Connection ended");
        }
    }else{
    LOGGER.info("Outward Data Migration is set to OFF Status");
    }
    }
    private static void downloadFile(String remotefrontImageLocation, String remoterearImageLocation,String frontImgName,String rearImgName,ChannelSftp channelSftp,boolean isOutward) throws FileNotFoundException, IOException, SftpException{
//System.out.println(frontImageLocation+":"+rearImageLocation);
        String appPropertiesPath = "DataMigrationAbabil.properties";
Properties appProps = new Properties();
appProps.load(new FileInputStream(appPropertiesPath));
String localfrontimage="D:\\TEMP\\"+frontImgName;
String localrearimage="D:\\TEMP\\"+rearImgName;
if(isOutward){
try{

channelSftp.get(remotefrontImageLocation, localfrontimage);
LOGGER.info(remotefrontImageLocation+" Successfully moved to"+localfrontimage);
channelSftp.get(remoterearImageLocation, localrearimage);
LOGGER.info(remoterearImageLocation+" Successfully moved to"+localrearimage);

}catch(Exception e){
if(isOutward){
LOGGER.info("Remote File Not Found! So, Trying to Read from Alternate Location");
remotefrontImageLocation=remotefrontImageLocation.replace("placement", "verify");
remoterearImageLocation=remoterearImageLocation.replace("placement", "verify");
System.out.println(remotefrontImageLocation);
channelSftp.get(remotefrontImageLocation, localfrontimage);
}

LOGGER.error("SFTP Connection Failed with ABABIL Server");
}
}else{
try{

channelSftp.get(remotefrontImageLocation, localfrontimage);
LOGGER.info(remotefrontImageLocation+" Successfully moved to"+localfrontimage);
channelSftp.get(remoterearImageLocation, localrearimage);
LOGGER.info(remoterearImageLocation+" Successfully moved to"+localrearimage);

}catch(Exception e){
e.printStackTrace();
LOGGER.error("SFTP Connection Failed with ABABIL Server");
}
}
}
    
}
