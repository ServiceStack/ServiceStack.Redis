using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{

    [TestFixture, Category("Integration")]
	public class AlchemyNativeClientTests: AlchemyClientTestsBase
	{
        public override void OnBeforeEachTest()
        {
            base.OnBeforeEachTest();

            const string tableName = "foo";
            const string columnDefinitions = "id INT, val FLOAT, name TEXT";
            Alchemy.CreateTable(GetBytes(tableName), GetBytes(columnDefinitions));
        }

        [TearDown]
        public void TearDown()
        {
            Alchemy.DropTable(GetBytes("foo"));
        }

        [Test]
        public void Can_CreateDropTable()
        {
            const string tableName = "foo_test";
            const string columnDefinitions = "id INT, val FLOAT, name TEXT";
            Alchemy.CreateTable	(GetBytes(tableName), GetBytes(columnDefinitions));
            Alchemy.DropTable(GetBytes(tableName));
        }
        [Test]
        public void Can_CreateIndex()
        {
            const string indexName = "foo_val_index";
            const string tableName = "foo";
            const string columnName = "val";

            Alchemy.CreateIndex(GetBytes(indexName), GetBytes(tableName), GetBytes(columnName));
        }
        [Test]
        public void Can_DropIndex()
        {
            const string indexName = "foo_val_index";
            Alchemy.DropIndex(GetBytes(indexName));
    
        }
        [Test]
        public void Can_Desc()
        {
            const string tableName = "foo";
            byte[][] results = Alchemy.Desc(GetBytes(tableName));
  
        }
        [Test]
        public void Can_Dump()
        {
            const string tableName = "foo";
            IntsertData();
            byte[][] results = Alchemy.Dump(GetBytes(tableName));
        }
        [Test]
        public void Can_DumpToMysql()
        {
            const string tableName = "foo";
            const string mysqlTableName = "foo";
            Alchemy.DumpToMysql(GetBytes(tableName), GetBytes(mysqlTableName));
     
        }
        [Test]
        public void Can_DumpToFile()
        {
            IntsertData();
            const string tableName = "foo";
            const string fileName = "foo.sql";
            Alchemy.DumpToFile(GetBytes(tableName), GetBytes(fileName));
        }
        [Test]
        public void Can_Insert()
        {
            IntsertData();
        }

        private void IntsertData()
        {
            const string tableName = "foo";
            const string values = "2,2.2222222,'two'";
            Alchemy.Insert(GetBytes(tableName), GetBytes(values));
        }

        [Test, Ignore]
        public void Can_InsertReturnSize()
        {
            const string tableName = "foo";
            const string values = "2,2.2222222,'two'";
            Alchemy.InsertReturnSize(GetBytes(tableName), GetBytes(values));
        }

        [Test, Ignore]
        public void Can_Select()
        {
            const string columnList = "*";
            const string tableName = "foo";
            const string whereClause = "id = 1";
            Alchemy.Select(GetBytes(columnList), GetBytes(tableName), GetBytes(whereClause));

        }
        [Test, Ignore]
        public void Can_ScanSelect()
        {
            const string columnList = "*";
            const string tableName = "foo";
            Alchemy.ScanSelect(GetBytes(columnList), GetBytes(tableName), null);
        }

        [Test]
        public void Can_Update()
        {
            const string tableName = "foo";
            const string updateList = "val=9.999999";
            const string whereClause = "id = 1";
            Alchemy.Update( GetBytes(tableName),GetBytes(updateList), GetBytes(whereClause));

        }
        [Test]
        public void Can_Delete()
        {
            const string tableName = "foo";
            const string whereClause = "id = 1";
            Alchemy.Delete( GetBytes(tableName), GetBytes(whereClause));
  
        }

        [Test, Ignore]
        public void Can_Lua()
        {
            const string luaCommand = "return select('*','foo','id = 1');";
            Alchemy.Lua(GetBytes(luaCommand));
        }
    }

}
