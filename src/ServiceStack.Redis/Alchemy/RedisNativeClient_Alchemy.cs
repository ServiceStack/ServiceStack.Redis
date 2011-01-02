using ServiceStack.Redis.Alchemy;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    public partial class RedisNativeClient
    {
        /// <summary>
        /// Interface to Alchemy DB
        /// http://code.google.com/p/alchemydatabase/
        /// </summary>
        public class AlchemyNativeClient : RedisNativeClient,IAlchemyNativeClient
        {

            public AlchemyNativeClient(string host)
                : this(host, DefaultPort)
            {
            }

            public AlchemyNativeClient(string host, int port)
                : base(host, port)
            {
            }

            public AlchemyNativeClient()
                : this(DefaultHost, DefaultPort)
            {
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="columnDefinitions"></param>
            public void CreateTable(byte[] tablename, byte[] columnDefinitions)
            {
                var columnDefinitionsString = "(" + columnDefinitions + ")";
                var cmdWithArgs = new byte[4][] { Alchemy.Commands.Create, Keywords.Table, tablename, columnDefinitionsString.ToUtf8Bytes() };
                SendExpectSuccess(cmdWithArgs);
            }
            /// <summary>
            ///  
            /// </summary>
            /// <param name="tablename"></param>
            /// <returns></returns>
            public int DropTable(byte[] tablename)
            {
                var cmdWithArgs = new byte[3][] { Alchemy.Commands.Drop, Keywords.Table, tablename };
                return SendExpectInt(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="indexname"></param>
            /// <param name="tablename"></param>
            /// <param name="column"></param>
            public void CreateIndex(byte[] indexname, byte[] tablename, byte[] column)
            {
                var columnString = "(" + column + ")";
                var cmdWithArgs = new byte[6][] { Alchemy.Commands.Create, Keywords.Index, indexname, Keywords.On, tablename, columnString.ToUtf8Bytes() };
                SendExpectSuccess(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="indexname"></param>
            /// <returns></returns>
            public int DropIndex(byte[] indexname)
            {
                var cmdWithArgs = new byte[3][] { Alchemy.Commands.Drop, Keywords.Index, indexname };
                return SendExpectInt(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <returns></returns>
            public byte[][] Desc(byte[] tablename)
            {
                var cmdWithArgs = new byte[2][] { Commands.Desc, tablename };
                return SendExpectMultiData(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <returns></returns>
            public byte[][] Dump(byte[] tablename)
            {
                var cmdWithArgs = new byte[2][] { Alchemy.Commands.Dump, tablename };
                return SendExpectMultiData(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="mysqlTablename"></param>
            public void DumpToMysql(byte[] tablename, byte[] mysqlTablename)
            {
                byte[][] cmdWithArgs;
                if (mysqlTablename != null && mysqlTablename.Length > 0)
                    cmdWithArgs = new byte[5][] { Alchemy.Commands.Dump, tablename, Keywords.To, Keywords.Mysql, mysqlTablename };
                else
                    cmdWithArgs = new byte[4][] { Alchemy.Commands.Dump, tablename, Keywords.To, Keywords.Mysql };
                SendExpectSuccess(cmdWithArgs);

            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="fileName"></param>
            public void DumpToFile(byte[] tablename, byte[] fileName)
            {
                var cmdWithArgs = new byte[5][] { Alchemy.Commands.Dump, tablename, Keywords.To, Keywords.File, fileName };
                SendExpectSuccess(cmdWithArgs);

            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="valuesList"></param>
            public void Insert(byte[] tablename, byte[] valuesList)
            {
                var valuesListString = "(" + valuesList + ")";
                var cmdWithArgs = new byte[5][] { Alchemy.Commands.Insert, Keywords.Into, tablename, Keywords.Values, valuesListString.ToUtf8Bytes() };
                SendExpectSuccess(cmdWithArgs);

            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="valuesList"></param>
            public void InsertReturnSize(byte[] tablename, byte[] valuesList)
            {
                var valuesListString = "(" + valuesList + ")";
                var cmdWithArgs = new byte[7][] { Alchemy.Commands.Insert, Keywords.Into, tablename, Keywords.Values, valuesListString.ToUtf8Bytes(), Keywords.Return, Keywords.Size };
                SendCommand(cmdWithArgs);

            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="columnList"></param>
            /// <param name="tablename"></param>
            /// <param name="whereClause"></param>
            /// <returns></returns>
            public byte[][] Select(byte[] columnList, byte[] tablename, byte[] whereClause)
            {
                var cmdWithArgs = new byte[6][] { Redis.Commands.Select, columnList, Keywords.From, tablename, Keywords.Where, whereClause };
                return SendExpectMultiData(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="columnList"></param>
            /// <param name="tablename"></param>
            /// <param name="whereClause"></param>
            /// <returns></returns>
            public byte[][] ScanSelect(byte[] columnList, byte[] tablename, byte[] whereClause)
            {
                byte[][] cmdWithArgs;
                if (whereClause != null && whereClause.Length > 0)
                    cmdWithArgs = new byte[6][] { Alchemy.Commands.ScanSelect, columnList, Keywords.From, tablename, Keywords.Where, whereClause };
                else
                    cmdWithArgs = new byte[4][] { Alchemy.Commands.ScanSelect, columnList, Keywords.From, tablename };

                return SendExpectMultiData(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="updateList"></param>
            /// <param name="whereClause"></param>
            /// <returns></returns>
            public int Update(byte[] tablename, byte[] updateList, byte[] whereClause)
            {
                var cmdWithArgs = new byte[6][] { Alchemy.Commands.Update, tablename, Redis.Commands.Set, updateList, Keywords.Where, whereClause };
                return SendExpectInt(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="tablename"></param>
            /// <param name="whereClause"></param>
            /// <returns></returns>
            public int Delete(byte[] tablename, byte[] whereClause)
            {
                var cmdWithArgs = new byte[5][] { Alchemy.Commands.Delete, Keywords.From, tablename, Keywords.Where, whereClause };
                return SendExpectInt(cmdWithArgs);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="command"></param>
            public void Lua(byte[] command)
            {
                var cmdWithArgs = new byte[2][] { Alchemy.Commands.Lua, command };
                SendExpectSuccess(cmdWithArgs);
            }
        }
    }
}
