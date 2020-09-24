using System;
using Npgsql;
using System.Data;
using System.Threading;

namespace BulkCopyPostGresPractice
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var table = new DataTable("practicetable2");
            table.Columns.Add("num", typeof(int));
            table.Columns.Add("numstring", typeof(string));
            table.Columns.Add("description", typeof(string));
            table.Columns.Add("someid", typeof(Guid));

            for (int i = 0; i < 10; i++)
            {
                var row = table.NewRow();
                row["num"] = i;
                row["numstring"] = i.ToString();
                // Commented below line to test for null case.
                // row["description"] = "sfsdf";
                row["someid"] = Guid.NewGuid();
                table.Rows.Add(row);
            }
            for (int i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                Console.WriteLine($"full name: {column.DataType.FullName}");
            }

            using (var conn = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=joyn_fdgcommon_dev;User Id=postgres;Password=admin;"))
            {
                conn.Open();
                using (NpgsqlBinaryImporter writer = conn.BeginBinaryImport("copy stage.practicetable2 from stdin(FORMAT BINARY)"))
                {
                    for (int i = 0; i < table.Rows.Count; i++)
                    {
                        DataRow row = table.Rows[i];
                        writer.StartRow();
                        for (int j = 0; j < table.Columns.Count; j++)
                        {
                            var column = table.Columns[j];
                            var value = row[column.ColumnName];
                            if (value != null)
                            {
                                writer.Write(row[column.ColumnName]);
                            }
                            else
                            {
                                Console.WriteLine("writing null");
                                writer.WriteNull();
                            }
                        }

                    }
                    writer.Complete();
                }
            }
        }
    }
}