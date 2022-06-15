using MySql.Data.MySqlClient;
using System;
using Dapper;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

namespace WInBackHeroSQLJob
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["mysqlConnection"].ConnectionString;
            var remoteConnectionString = ConfigurationManager.ConnectionStrings["remoteMSSqlConnection"].ConnectionString;
            var batchLimit = Convert.ToInt32(ConfigurationManager.AppSettings["BatchLimitPerInsert"]);
            var mySQLDB = ConfigurationManager.AppSettings["MySqlTableName"];
            var remoteMSSQLDB =ConfigurationManager.AppSettings["remoteMSSqlTableName"];


            Console.WriteLine("Starting WinBack Data Job");
            Console.WriteLine("Connecting to My SQL DB");

            try {

            
            using (var con = new MySqlConnection(connectionString))
            {
                    Console.WriteLine(" ");
                    Console.WriteLine("Successfully Connected to My SQL DB");
                    Console.WriteLine("\n Do you want to truncate mysql table before Data migration ? Please type 'yes' for confirmation ");
                    var input = Console.ReadLine();
                    if( String.Compare(input,"yes",true) == 0 )
                    {
                        var afRows = 0; con.Execute(String.Format("TRUNCATE TABLE {0}", mySQLDB));
                        Console.WriteLine("Truncate Successful {0} rows affected", afRows);
                    }
                    else Console.WriteLine("continuing without truncate");
                     
                  var lastidinserted = 0;

                    Console.WriteLine("Connecting to remote MS SQL DB");
                    using (var remoteCon = new SqlConnection(remoteConnectionString))
                {
                       var totalCount = remoteCon.ExecuteScalar<int>(String.Format("SELECT max(id) FROM {0};", remoteMSSQLDB));
                        var totalRowCount = remoteCon.ExecuteScalar<int>(String.Format("SELECT count(id) FROM {0};", remoteMSSQLDB));
                        Console.WriteLine("\n last row Id in remote db is " + totalCount);
                        Console.WriteLine("\n total Row count in remote db " + totalRowCount);


                        for (int i = 0; i <= totalCount; i = i + batchLimit)
                        {
                            DataTable dt = new DataTable();
                            lastidinserted = con.ExecuteScalar<int>(String.Format("SELECT id FROM {0} order by id desc limit 1;", mySQLDB));
                            Console.WriteLine("\n ID of last batch inserted row" + lastidinserted);

                            #region Fetch rows

                            using (SqlCommand cmd = new SqlCommand(String.Format("SELECT * FROM {0} TK(nolock)  where TK.id >" + lastidinserted + "  order by id asc OFFSET " + 0 + " ROWS FETCH NEXT " + batchLimit + " ROWS ONLY",remoteMSSQLDB), remoteCon))
                            {
                                cmd.CommandTimeout = 300;
                                cmd.CommandType = CommandType.Text;
                                using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                                {
                                   var result = sda.Fill(dt);
                                    Console.Write("Successfully fetched rows from remote DB");
                                }
                            }

                            Console.WriteLine("records fetch successful");
                            Console.WriteLine(dt.Rows.Count);
                            #endregion

                            #region insert 

                            string firstCellValue = dt.Rows[0][0].ToString();
                            string lastCellValue = dt.Rows[dt.Rows.Count - 1][0].ToString();
                            // long batchId = batchStartinsert(Convert.ToInt64(firstCellValue));

                          Console.WriteLine("\n\n -------- Inserting to MYSQl  From Index {0}  To Index  {1} -------------: ", firstCellValue, lastCellValue);


                            //DataColumn newCol = new DataColumn("batchId", typeof(long));
                            //newCol.AllowDBNull = true;
                            //dt.Columns.Add(newCol);

                            string rows = Program.BulkInsert(ref dt, mySQLDB);
                            rows.Replace(", ,", ",'',");
                            rows.Replace(",,", ",'',");
                            //Console.WriteLine(rows);

                            var result2 = con.Execute(rows);

                            Console.WriteLine("\n\n Number of rows inserted {0} -------------: ", result2);
                            //batchENDinsert(batchId,Convert.ToInt64(lastCellValue));
                            Console.WriteLine("\n\n -------- Done Inserting to MYSQl  From Index {0} To Index {1}-------------: ", firstCellValue, lastCellValue);

                        }
                       
                        #endregion

                    }
            }

                Console.WriteLine("Data Migration complete");
        }
            catch(Exception ex)
            {
                Console.WriteLine("Error occured while executing " + ex.Message);
            }
            finally
            {
                Console.ReadLine();
            }
        }


        public static String BulkInsert(ref DataTable table, String table_name)
        {
            try
            {
                StringBuilder queryBuilder = new StringBuilder();
                DateTime dt;

                queryBuilder.AppendFormat("INSERT INTO `{0}` (", table_name);

                // more than 1 column required and 1 or more rows
                if (table.Columns.Count > 1 && table.Rows.Count > 0)
                {
                    // build all columns
                    queryBuilder.AppendFormat("`{0}`", table.Columns[0].ColumnName.Replace(" ", ""));

                    if (table.Columns.Count > 1)
                    {
                        for (int i = 1; i < table.Columns.Count; i++)
                        {
                            queryBuilder.AppendFormat(", `{0}` ", table.Columns[i].ColumnName.Replace(" ",""));
                        }
                    }

                    queryBuilder.AppendFormat(") VALUES (");

                    // build all values for the first row
                    // escape String & Datetime values!
                    if (table.Columns[0].DataType == typeof(String))
                    {
                        queryBuilder.AppendFormat("'{0}'", MySqlHelper.EscapeString(table.Rows[0][table.Columns[0].ColumnName]?.ToString() ?? "''") ?? "''");
                    }
                    else if (table.Columns[0].DataType == typeof(DateTime))
                    {
                        if (table.Rows[0][table.Columns[0].ColumnName].ToString() != "" && table.Rows[0][table.Columns[0].ColumnName] != DBNull.Value)
                        {
                            dt = (DateTime)table.Rows[0][table.Columns[0].ColumnName];
                            queryBuilder.AppendFormat("'{0}'", dt.ToString("yyyy-MM-dd"));
                        }
                        else
                        {
                            queryBuilder.AppendFormat("null");

                        }
                    }
                    else if (table.Columns[0].DataType == typeof(Int32))
                    {
                        queryBuilder.AppendFormat("{0}", table.Rows[0].Field<Int32?>(table.Columns[0].ColumnName) ?? 0);
                    }
                    else if (table.Columns[0].DataType == typeof(Int64))
                    {
                        queryBuilder.AppendFormat("{0}", table.Rows[0].Field<Int64?>(table.Columns[0].ColumnName) ?? 0);
                    }
                    else if (table.Columns[0].DataType == typeof(decimal))
                    {
                        queryBuilder.AppendFormat("{0}", table.Rows[0].Field<decimal?>(table.Columns[0].ColumnName) ?? 0);
                    }
                    else
                    {
                        queryBuilder.AppendFormat("{0}", "s");
                    }

                    for (int i = 1; i < table.Columns.Count; i++)
                    {
                        // escape String & Datetime values!
                        if (table.Columns[i].DataType == typeof(String))
                        {
                            queryBuilder.AppendFormat(", '{0}'", MySqlHelper.EscapeString(table.Rows[0][table.Columns[i].ColumnName]?.ToString() ?? "''") ?? "''");
                        }
                        else if (table.Columns[i].DataType == typeof(DateTime))
                        {
                            if (table.Rows[0][table.Columns[i].ColumnName].ToString() != "" && table.Rows[0][table.Columns[i].ColumnName] != DBNull.Value)
                            {
                                dt = (DateTime)table.Rows[0][table.Columns[i].ColumnName];
                                queryBuilder.AppendFormat(", '{0}'", dt.ToString("yyyy-MM-dd"));
                            }
                            else
                            {
                                queryBuilder.AppendFormat(", null");
                            }

                        }
                        else if (table.Columns[i].DataType == typeof(Int64))
                        {
                            queryBuilder.AppendFormat(", {0}", table.Rows[0].Field<Int64?>(table.Columns[i].ColumnName) ?? 0);
                        }
                        else if (table.Columns[i].DataType == typeof(Int32))
                        {
                            queryBuilder.AppendFormat(", {0}", table.Rows[0].Field<Int32?>(table.Columns[i].ColumnName) ?? 0);
                        }
                        else if (table.Columns[i].DataType == typeof(decimal))
                        {
                            queryBuilder.AppendFormat(", {0}", table.Rows[0].Field<decimal?>(table.Columns[i].ColumnName) ?? 0);
                        }
                        else
                        {
                            queryBuilder.AppendFormat(", {0}", String.IsNullOrEmpty(table.Rows[0][table.Columns[i].ColumnName]?.ToString())?"''" : table.Rows[0][table.Columns[i].ColumnName]?.ToString());
                        }
                    }

                    queryBuilder.Append(")");
                    queryBuilder.AppendLine();

                    // build all values all remaining rows
                    if (table.Rows.Count > 1)
                    {
                        // iterate over the rows
                        for (int row = 1; row < table.Rows.Count; row++)
                        {
                            // open value block
                            queryBuilder.Append(", (");

                            // escape String & Datetime values!
                            if (table.Columns[0].DataType == typeof(String))
                            {
                                queryBuilder.AppendFormat("'{0}'", MySqlHelper.EscapeString(table.Rows[row][table.Columns[0].ColumnName].ToString()));
                            }
                            else if (table.Columns[0].DataType == typeof(DateTime))
                            {
                                if (table.Rows[row][table.Columns[0].ColumnName].ToString() != "" && table.Rows[row][table.Columns[0].ColumnName] != DBNull.Value)
                                {
                                    dt = (DateTime)table.Rows[row][table.Columns[0].ColumnName];
                                    queryBuilder.AppendFormat("'{0}'", dt.ToString("yyyy-MM-dd"));
                                }
                                else
                                {
                                    queryBuilder.AppendFormat(", null");
                                }
                            }
                            else if (table.Columns[0].DataType == typeof(Int32))
                            {
                                queryBuilder.AppendFormat("{0}", table.Rows[row].Field<Int32?>(table.Columns[0].ColumnName) ?? 0);
                            }
                            else if (table.Columns[0].DataType == typeof(Int64))
                            {
                                queryBuilder.AppendFormat("{0}", table.Rows[row].Field<Int64?>(table.Columns[0].ColumnName) ?? 0);
                            }
                            else if (table.Columns[0].DataType == typeof(decimal))
                            {
                                queryBuilder.AppendFormat("{0}", table.Rows[row].Field<decimal?>(table.Columns[0].ColumnName) ?? 0);
                            }
                            else
                            {
                                queryBuilder.AppendFormat(", {0}", String.IsNullOrEmpty(table.Rows[0][table.Columns[0].ColumnName]?.ToString()) ? "''" : table.Rows[0][table.Columns[0].ColumnName]?.ToString());
                            }

                            for (int col = 1; col < table.Columns.Count; col++)
                            {
                                // escape String & Datetime values!
                                if (table.Columns[col].DataType == typeof(String))
                                {
                                    queryBuilder.AppendFormat(", '{0}'", MySqlHelper.EscapeString(table.Rows[row][table.Columns[col].ColumnName]?.ToString() ?? "''"));
                                }
                                else if (table.Columns[col].DataType == typeof(DateTime))
                                {
                                    if (table.Rows[row][table.Columns[col].ColumnName].ToString() != "" && table.Rows[row][table.Columns[col].ColumnName] != DBNull.Value)
                                    {
                                        dt = (DateTime)table.Rows[row][table.Columns[col].ColumnName];
                                        queryBuilder.AppendFormat(", '{0}'", dt.ToString("yyyy-MM-dd"));
                                    }
                                    else
                                    {
                                        queryBuilder.AppendFormat(", null");

                                    }
                                }
                                else if (table.Columns[col].DataType == typeof(Int32))
                                {
                                    queryBuilder.AppendFormat(", {0}", table.Rows[row].Field<Int32?>(table.Columns[col].ColumnName) ?? 0);
                                }
                                else if (table.Columns[col].DataType == typeof(Int64))
                                {
                                    queryBuilder.AppendFormat(", {0}", table.Rows[row].Field<Int64?>(table.Columns[col].ColumnName) ?? 0);
                                }
                                else if (table.Columns[col].DataType == typeof(decimal))
                                {
                                    queryBuilder.AppendFormat(", {0}", table.Rows[row].Field<decimal?>(table.Columns[col].ColumnName) ?? 0);
                                }
                                else
                                {
                                    queryBuilder.AppendFormat(", {0}", table.Rows[row][table.Columns[col].ColumnName]?.ToString() ?? "''" );
                                }

                            } // end for (int i = 1; i < table.Columns.Count; i++)

                            // close value block
                            queryBuilder.Append(")");
                            queryBuilder.AppendLine();

                        } // end for (int r = 1; r < table.Rows.Count; r++)

                        // sql delimiter =)
                        queryBuilder.Append(";");

                    } // end if (table.Rows.Count > 1)

                    return queryBuilder.ToString();
                }
                else
                {
                    return "";
                } // end if(table.Columns.Count > 1 && table.Rows.Count > 0)
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return "";
            }
        }
    }
}
