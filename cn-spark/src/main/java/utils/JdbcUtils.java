package utils;

import java.sql.*;

public class JdbcUtils {
	private static String url = "jdbc:mysql://oym2.com:3306/mysql?createDatabaseIfNotExist=true&amp;useSSL=false&amp;autoReconnect=true&amp;failOverReadOnly=false&amp;maxReconnects=10&amp;characterEncoding=UTF-8";
	private static String user = "root";
	private static String pwd = "miao";

	private JdbcUtils() {

	}

	public static void main(String[] args){
		try{
			Connection conn=getConnection();
          /*  String sql = "insert into textFile (value) values(?)";
            PreparedStatement pstmt;
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            pstmt.setString(1,"test");
            pstmt.executeUpdate();
            free(pstmt,conn);*/
			System.out.println("******"+conn);
		}catch (SQLException e){
			e.printStackTrace();
		}
	}


	// 1、注册驱动oracle.jdbc.driver.OracleDriver
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("数据库驱动加载失败！");
		}
	}

	// 2、建立一个连接
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, pwd);
	}


    // 3、关闭资源
    public static void free(Statement stmt, Connection conn) {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }
	// 3、关闭资源2
	public static void free2(ResultSet rs, Statement stmt, Connection conn) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}