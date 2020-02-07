using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace CompareCounts
{
    public class ComparisonDetail
    {
        public string TableName { get; set; }
        public int SourceRows { get; set; }
        public int TargetRows { get; set; }
        public bool AllRowsTransfered { get { return SourceRows == TargetRows ? true : false; } }
        public string ErrorMessage { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            CompareCountsNow();
            Console.ReadKey();
        }
        static async void CompareCountsNow()
        {
            
        
            string src = "";
            string tgt = "";
            SqlConnection sourceConnection = new SqlConnection(src);
            SqlConnection targetConnection = new SqlConnection(tgt);
            IList<ComparisonDetail> tableDetails = new List<ComparisonDetail>();
            try
            {
                targetConnection.Open();
                sourceConnection.Open();
                foreach (string tableName in GetTableList(targetConnection))
                {
                    var tab = await CompareATable(new ComparisonDetail { TableName = tableName }, sourceConnection, targetConnection);
                    tableDetails.Add(tab);
                    if (!tab.AllRowsTransfered)
                        Console.WriteLine($"{tab.TableName}|source:{tab.SourceRows}|target:{tab.TargetRows}");
                }
                //foreach (var item in tableDetails)
                //{
                //    if (!item.AllRowsTransfered)
                //    {
                //        Console.WriteLine("*******************************************************************");
                //        Console.WriteLine($"{item.TableName}|source:{item.SourceRows}|target:{item.TargetRows}");
                //    }
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                targetConnection.Close();
                sourceConnection.Close();
            }
        }

        private async static Task<ComparisonDetail> CompareATable(ComparisonDetail table, SqlConnection sourceConnection, SqlConnection targetConnection)
        {
            //Console.WriteLine($"{table.TableName}");
            try
            {
                Task<int> src = GetRowCount(table.TableName, sourceConnection);
                Task<int> tgt = GetRowCount(table.TableName, targetConnection);
                table.SourceRows = await src;
                table.TargetRows = await tgt;
            }
            catch (Exception ex)
            {
                //failed to compare
                table.ErrorMessage = ex.Message;
            }
            return table;
        }

        static async Task<int> GetRowCount(string tableName, SqlConnection connection)
        {            
            var sql = $"SELECT COUNT(*) FROM {tableName}";
            using var cmd = new SqlCommand(sql, connection);
            var result = await cmd.ExecuteScalarAsync();
            int rows = Convert.ToInt32(result);
            //if (rows == 0)
            //    Console.WriteLine($"{tableName}-{connection.DataSource}--{rows}");
            return rows;
        }

        static IList<string> GetTableList(SqlConnection connection)
        {
            IList<string> names = new List<string>();
            var sql = $"SELECT name FROM sys.tables";
            using var cmd = new SqlCommand(sql, connection);
            SqlDataReader read = cmd.ExecuteReader();
            while (read.Read())
                names.Add(read.GetString(0));
            read.Close();
            return names;
        }
    }
}
