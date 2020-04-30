//package com.jy.test;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.Text;
//
//public class TestDBInputFormat {
//	public static void configureDB(JobConf job, String driverClass, String dbUrl
//			      , String userName, String passwd) {
//
//			//    job.set(DRIVER_CLASS_PROPERTY, driverClass);
//			//    job.set(URL_PROPERTY, dbUrl);
//			//    if(userName != null)
//			//      job.set(USERNAME_PROPERTY, userName);
//			//    if(passwd != null)
//			//      job.set(PASSWORD_PROPERTY, passwd);    
//			  }
//
//
//
//			    public static void setInput(JobConf job, Class<? extends DBWritable> inputClass, List<Object> conditions) {
//					job.setInputFormat(DBInputFormat.class);
//
//					DBConfiguration dbConf = new DBConfiguration(job);
//					dbConf.setInputClass(inputClass);
//
//					DBInputFormat.DBNodes = conditions.size();
//					objs = conditions;
//			    }
//
//				public static int DBNodes = 0;
//				public static List<Object> objs = null;
//
//			public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
//
//				InputSplit[] splits = new InputSplit[DBNodes];
//					long tmpstart = 0l;
//					long tmpend = 0l;
//
//					for (int i = 0; i < DBNodes; i++) {
//						DBInputSplit split;
//						
//							String tmpurl = "";
//							String tmpsql = "";
//							String tmppasswd = "";
//							String tmpdriverclass = "";
//							String tmpusername = "";
//			//               job.set(DRIVER_CLASS_PROPERTY, driverClass);
//			//                job.set(URL_PROPERTY, dbUrl);
//			//                if(userName != null)
//			//                  job.set(USERNAME_PROPERTY, userName);
//			//                if(passwd != null)
//			//                  job.set(PASSWORD_PROPERTY, passwd);            
//			            Object dto = objs.get(i);
//			            
//			            Field[] fields = dto.getClass().getDeclaredFields();
//			            for (Field field : fields) {
//
//			                field.setAccessible(true);
//			                
//			                try {
//
//			                    if (field.get(dto) != null) {
//
//			                        if (field.getName().equalsIgnoreCase("url")) {
//			                            
//			                            tmpurl = String.valueOf(field.get(dto));
//			                            job.set(DBConfiguration.URL_PROPERTY, tmpurl);
//			                        }
//			                        if (field.getName().equalsIgnoreCase("sql")) {
//
//			                            tmpsql=String.valueOf(field.get(dto));
//			                        }
//			                        if (field.getName().equalsIgnoreCase("passwd")) {
//
//			                            tmppasswd=String.valueOf(field.get(dto));
//			                            job.set(DBConfiguration.PASSWORD_PROPERTY, tmppasswd);
//			                        }
//			                        if (field.getName().equalsIgnoreCase("driverclass")) {
//
//			                            tmpdriverclass=String.valueOf(field.get(dto));
//			                            job.set(DBConfiguration.DRIVER_CLASS_PROPERTY, tmpdriverclass);
//			                        }
//			                        if (field.getName().equalsIgnoreCase("username")) {
//			                            
//			                            tmpusername=String.valueOf(field.get(dto));
//			                            job.set(DBConfiguration.USERNAME_PROPERTY, tmpusername);
//			                        }
//			                    }
//			                    else {
//			                        field.setAccessible(false);
//			                        continue;
//			                    }
//			                } catch (IllegalArgumentException e) {
//			                    // TODO Auto-generated catch block
//			                    e.printStackTrace();
//			                } catch (IllegalAccessException e) {
//			                    // TODO Auto-generated catch block
//			                    e.printStackTrace();
//			                }
//			                
//			                field.setAccessible(false);
//			                
//			            }
//			            try {
//			                Class.forName(job.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
//			            } catch (ClassNotFoundException e) {
//			                // TODO Auto-generated catch block
//			                e.printStackTrace();
//			            }
//			            Connection connection = null;
//			            Statement statement = null;
//			            ResultSet rs = null;
//			            try {
//			                connection = DriverManager.getConnection(job.get(DBConfiguration.URL_PROPERTY), 
//			                          job.get(DBConfiguration.USERNAME_PROPERTY), 
//			                          job.get(DBConfiguration.PASSWORD_PROPERTY));
//			                statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
//			                        ResultSet.CONCUR_READ_ONLY);
//			                
//			                String subSql = "select count(*) as count " +  tmpsql.substring(tmpsql.indexOf("from"));
//			                // statement.setFetchSize(Integer.MIN_VALUE);
//			                
//			                rs = statement.executeQuery(subSql);
//			                rs.next();
//			                if(rs.wasNull()){
//			                    
//			                    continue;
//			                }
//			                
//			                tmpend += Long.valueOf(rs.getString("count"));
//			                statement.close();
//			                rs.close();
//			                connection.close();
//			                
//			            } catch (SQLException e) {
//			                e.printStackTrace();
//			            } 
//			            
//			            split = new DBInputSplit(tmpstart, tmpend);
//			            split.setUrl(tmpurl);
//			            split.setSql(tmpsql);
//			            split.setPasswd(tmppasswd);
//			            split.setDriverClass(tmpdriverclass);
//			            split.setUserName(tmpusername);
//			            splits[i] = split;
//			        }
//			        return splits;
//			    }
//
//
//
//			    public DBInputSplit(long start, long end) {
//			            this.start = start;
//			            this.end = end;
//			        }
//
//			        /** {@inheritDoc} */
//			        public String[] getLocations() throws IOException {
//			            // TODO Add a layer to enable SQL "sharding" and support locality
//			            return new String[] {};
//			        }
//
//			        public String getUrl() {
//			            return url;
//			        }
//
//			        public void setUrl(String url) {
//			            this.url = url;
//			        }
//
//			        public String getSql() {
//			            return sql;
//			        }
//
//			        public void setSql(String sql) {
//			            this.sql = sql;
//			        }
//
//			        public String getUserName() {
//			            return userName;
//			        }
//
//			        public void setUserName(String userName) {
//			            this.userName = userName;
//			        }
//
//			        public String getPasswd() {
//			            return passwd;
//			        }
//
//			        public void setPasswd(String passwd) {
//			            this.passwd = passwd;
//			        }
//
//			        public String getDriverClass() {
//			            return driverClass;
//			        }
//
//			        public void setDriverClass(String driverClass) {
//			            this.driverClass = driverClass;
//			        }
//
//			        /**
//			         * @return The index of the first row to select
//			         */
//			        public long getStart() {
//			            return start;
//			        }
//
//			        /**
//			         * @return The index of the last row to select
//			         */
//			        public long getEnd() {
//			            return end;
//			        }
//
//			        /**
//			         * @return The total row count in this split
//			         */
//			        public long getLength() throws IOException {
//			            return end - start;
//			        }
//
//			        /** {@inheritDoc} */
//			        public void readFields(DataInput input) throws IOException {
//			            this.url=Text.readString(input);
//			            this.sql=Text.readString(input);
//			            this.userName=Text.readString(input);
//			            this.passwd=Text.readString(input);
//			            this.driverClass=Text.readString(input);
//			            start = input.readLong();
//			            end = input.readLong();
//			        }
//
//			        /** {@inheritDoc} */
//			        public void write(DataOutput output) throws IOException {
//			            Text.writeString(output, this.url);
//			            Text.writeString(output, this.sql);
//			            Text.writeString(output, this.userName);
//			            Text.writeString(output, this.passwd);
//			            Text.writeString(output, this.driverClass);
//			            output.writeLong(start);
//			            output.writeLong(end);
//			        }
//}
//			    }
//
//			protected DBRecordReader(DBInputSplit split, Class<T> inputClass, JobConf job) throws SQLException {
//						this.inputClass = inputClass;
//						this.split = split;
//						this.job = job;
//
//						try {
//							Class.forName(split.getDriverClass());
//						} catch (ClassNotFoundException e) {
//							e.printStackTrace();
//						}
//						myConnection = DriverManager.getConnection(split.getUrl(),
//								split.getUserName(), split.getPasswd());
//
//						statement = myConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
//								ResultSet.CONCUR_READ_ONLY);
//
//						// statement.setFetchSize(Integer.MIN_VALUE);
//						results = statement.executeQuery(split.getSql());
//			        }
//}
