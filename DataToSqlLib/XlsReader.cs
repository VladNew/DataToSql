// © 2023 Istomin Vlad <v.v.istomin@gmail.com>

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Linq;

namespace DataToSqlLib
{
	public class XlsReader
	{
		private DataSet Data;
		private string FilePath;
		private bool Preview;
		private OleDbDataAdapter Adapt;
		private string TableName;
		private string StrConnect;
		private bool IsDbf;
		private bool RecCount;
		private int Interval;

		private void AdaptFill(int minRow, int maxRow)
		{
			OleDbDataAdapter oleDbDataAdapter = new OleDbDataAdapter("SELECT * FROM " + this.TableName + " WHERE Recno()>" + minRow.ToString() + " AND  Recno()=<" + maxRow.ToString(), this.StrConnect);
			DataSet dataSet = new DataSet();
			oleDbDataAdapter.Fill(dataSet, "data");
			this.Data.Dispose();
			this.Data = dataSet;
		}

		private void InitVar()
		{
			this.Interval = 15000;
		}

		private void loadData(string pathFile)
		{
			string selectConnectionString;
			string excelProviderName = GetExcelProviderName();
			if (String.IsNullOrEmpty(excelProviderName))
			{
				excelProviderName = "=Microsoft.ACE.OLEDB.16.0";

			}
			selectConnectionString = @"Provider=" + excelProviderName + @";Data Source=" + pathFile + @";Extended Properties=""Excel 8.0;IMEX=1""";

			OleDbDataAdapter oleDbDataAdapter = new OleDbDataAdapter("SELECT * FROM [Лист1$]", selectConnectionString);
			this.StrConnect = selectConnectionString;
			DataSet dataSet = new DataSet();
			oleDbDataAdapter.Fill(dataSet, "ExcelInfo");
			this.Data = dataSet;
		}

		private string GetExcelProviderName()
		{
			var reader = OleDbEnumerator.GetRootEnumerator();

			var list = new List<String>();
			while (reader.Read())
			{
				for (var i = 0; i < reader.FieldCount; i++)
				{
					if (reader.GetName(i) == "SOURCES_NAME")
					{
						list.Add(reader.GetValue(i).ToString());
					}
				}
			}

			var excelProvider = list.Where(x => x.Contains("Microsoft.ACE.OLEDB")).FirstOrDefault();

			return excelProvider;
		}

		private void loadData(string pathFile, string tableName)
		{
			string selectConnectionString = "PROVIDER=vfpoledb;Data Source=" + pathFile;
			this.TableName = tableName;
			OleDbDataAdapter oleDbDataAdapter = new OleDbDataAdapter("SELECT * FROM " + tableName, selectConnectionString);
			this.StrConnect = selectConnectionString;
			DataSet dataSet = new DataSet();
			oleDbDataAdapter.Fill(dataSet, "ExcelInfo");
			this.Data = dataSet;
		}

		private void loadData(string pathFile, string tableName, bool preview)
		{
			this.Preview = preview;
			string selectConnectionString = "PROVIDER=vfpoledb;Data Source=" + pathFile;
			this.TableName = tableName;
			DataSet dataSet = new DataSet();
			OleDbDataAdapter oleDbDataAdapter = new OleDbDataAdapter("SELECT * FROM " + tableName, selectConnectionString);
			this.Adapt = oleDbDataAdapter;
			this.StrConnect = selectConnectionString;
			if (!preview)
				oleDbDataAdapter.Fill(dataSet, "ExcelInfo");
			else
				oleDbDataAdapter.Fill(dataSet, 0, this.Interval, "ExcelInfo");
			this.Data = dataSet;
		}

		public XlsReader(string pathFile)
		{
			this.IsDbf = false;
			this.loadData(pathFile);
		}

		public XlsReader(string pathFile, bool isDbf)
		{
			this.InitVar();
			this.IsDbf = isDbf;
			FileInfo fileInfo = new FileInfo(pathFile);
			string tableName = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4);
			if (isDbf)
			{
				this.loadData(pathFile, tableName);
			}
			else
			{
				loadData(pathFile);
			}

		}

		public XlsReader(string pathFile, bool isDbf, bool IsPreview)
		{
			this.InitVar();
			FileInfo fileInfo = new FileInfo(pathFile);
			string tableName = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4);
			this.loadData(pathFile, tableName, IsPreview);
		}

		public XlsReader(string pathFile2, bool isDbf, string DbfTable)
		{
			this.InitVar();
			this.IsDbf = isDbf;
			if (!isDbf)
				this.loadData(pathFile2);
			else
				this.loadData(pathFile2, DbfTable);
		}

		public DataSet getDataSet()
		{
			return this.Data;
		}

		public void DataToSl(string sqlCon, string tableName)
		{
			this.DataToSl(sqlCon, tableName, true);
		}

		public void DataToSqlExt(string sqlCon, string tableName, bool overwriteTable)
		{
			int interval = this.Interval;
			if (this.Preview)
			{
				this.DataToSl(sqlCon, tableName, overwriteTable);
				int maxRow = interval + this.Interval;
				this.AdaptFill(maxRow - this.Interval, maxRow);
				while (this.Data.Tables[0].Rows.Count > 0)
				{
					this.DataToSl(sqlCon, tableName, false);
					maxRow += this.Interval;
					this.AdaptFill(maxRow - this.Interval, maxRow);
				}
			}
			else
				this.DataToSl(sqlCon, tableName, overwriteTable);
		}

		public void DataToSl(string sqlCon, string tableName, bool overwriteTable)
		{
			string schemaName = "dbo";
			if (overwriteTable)
			{
				DataTable table = this.Data.Tables["ExcelInfo"];
				string str = sqlCon;
				SqlCommand sqlCommand = new SqlCommand(XlsReader.CreateDdlFromDataTable(schemaName, tableName, table, str), new SqlConnection(str));
				sqlCommand.Connection.Open();
				sqlCommand.ExecuteNonQuery();
				sqlCommand.Dispose();
			}
			using (SqlConnection connection = new SqlConnection(sqlCon))
			{
				connection.Open();
				using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connection))
				{
					sqlBulkCopy.DestinationTableName = schemaName + "." + tableName;
					DataTable table = this.Data.Tables[0];
					sqlBulkCopy.WriteToServer(table);
					connection.Close();
				}
			}
		}

		private static string CreateDdlFromDataTable(
		  string schemaName,
		  string tableName,
		  DataTable dt,
		  string sqlConnectString)
		{
			StringBuilder stringBuilder = new StringBuilder("IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id  = object_id('[" + schemaName + "].[" + tableName + "]') AND OBJECTPROPERTY(id, 'IsUserTable')  =  1)" + Environment.NewLine + "DROP TABLE [" + schemaName + "].[" + tableName + "];" + Environment.NewLine + Environment.NewLine);
			stringBuilder.Append("CREATE TABLE [" + schemaName + "].[" + tableName + "] (" + Environment.NewLine);
			foreach (DataColumn column in (InternalDataCollectionBase)dt.Columns)
			{
				stringBuilder.Append("[" + column.ColumnName + "] ");
				stringBuilder.Append(XlsReader.NetType2SqlType(column.DataType.ToString(), column.MaxLength) + " ");
				if (column.AutoIncrement)
					stringBuilder.Append("IDENTITY ");
				stringBuilder.Append((column.AllowDBNull ? "" : "NOT ") + "NULL," + Environment.NewLine);
			}
			stringBuilder.Remove(stringBuilder.Length - (Environment.NewLine.Length + 1), 1);
			stringBuilder.Append(") ON [PRIMARY];" + Environment.NewLine + Environment.NewLine);
			return stringBuilder.ToString();
		}

		private static string NetType2SqlType(string netType, int maxLength)
		{
			if (maxLength < 1)
				maxLength = 512;
			string str;
			switch (netType)
			{
				case "System.Boolean":
					str = "[bit]";
					break;
				case "System.Byte":
					str = "[tinyint]";
					break;
				case "System.Int16":
					str = "[smallint]";
					break;
				case "System.Int32":
					str = "[int]";
					break;
				case "System.Int64":
					str = "[bigint]";
					break;
				case "System.Byte[]":
					str = "[binary]";
					break;
				case "System.Char[]":
					str = "[nchar] (" + (object)maxLength + ")";
					break;
				case "System.String":
					str = maxLength != 1073741823 ? "[nvarchar] (" + (object)maxLength + ")" : "[ntext]";
					break;
				case "System.Single":
					str = "[real]";
					break;
				case "System.Double":
					str = "[float]";
					break;
				case "System.Decimal":
					str = "[float]";
					break;
				case "System.DateTime":
					str = "[datetime]";
					break;
				case "System.Guid":
					str = "[uniqueidentifier]";
					break;
				case "System.Object":
					str = "[sql_variant]";
					break;
				default:
					str = "[not supported]";
					break;
			}
			return str;
		}




	}
}
