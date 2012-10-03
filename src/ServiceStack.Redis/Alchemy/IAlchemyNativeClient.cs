namespace ServiceStack.Redis.Alchemy
{
    /// <summary>
    /// Native interface to Alchemy DB
    /// </summary>
     interface IAlchemyNativeClient : IRedisNativeClient
    {
         void CreateTable(byte[] tablename, byte[] columnDefinitions);
         int DropTable(byte[] tablename);
         void CreateIndex(byte[] indexname, byte[] tablename, byte[] column);
         int DropIndex(byte[] indexname);
         byte[][] Desc(byte[] tablename);
         byte[][] Dump(byte[] tablename);
         void DumpToMysql(byte[] tablename, byte[] mysqlTablename);
         void DumpToFile(byte[] tablename, byte[] fileName);
         void Insert(byte[] tablename, byte[] valuesList);
         void InsertReturnSize(byte[] tablename, byte[] valuesList);
         byte[][] Select(byte[] columnList, byte[] tablename, byte[] whereClause);
         byte[][] ScanSelect(byte[] columnList, byte[] tablename, byte[] whereClause);
         int Update(byte[] tablename, byte[] updateList, byte[] whereClause);
         int Delete(byte[] tablename, byte[] whereClause);
         void Lua(byte[] command);
    }
}
