using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using Bogus;
using Dapper;
using Microsoft.SqlServer.Types;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;
using static System.Console;

const double MostSouthLat = 47.48667229881244;
const double MostNorthLat = 54.84800488971114;
const double MostWestLong = 6.026129589265816;
const double MostEastLong = 14.540386639801365;

using SqlConnection connection = new ("Data Source=DESKTOP-P04REE9;Database=ServicePortal;Integrated Security=true;");
connection.Open();

var client = new MongoClient("mongodb://localhost:27017");
var db =  client.GetDatabase("ServicePortal");
var collection = db.GetCollection<Offer>("Offers");

var faker = new Faker();
var random = new Random();

//collection.DeleteMany(_ => true);
//InsertNewRandomOffers(File.ReadAllLines("words.txt"), count: 10000, repeats: 200);
//GenerateCategories();
GenerateRandomSqlOffers(1_999_990, File.ReadAllLines("german-words.txt"));
//AssignRandomCategories();
//InsertKeywords(File.ReadAllLines("words.txt"));
//AssignKeywords();
//InsertKeywords(File.ReadAllLines("words.txt"), 1);
//InsertKeywords(File.ReadAllLines("words.txt"), 2);
//InsertKeywords(File.ReadAllLines("words.txt"), 3);
//InsertKeywords(File.ReadAllLines("words.txt"), 4);
//InsertKeywords(File.ReadAllLines("words.txt"), 5);
//InsertKeywords(File.ReadAllLines("words.txt"), 6);

//RepeatTest(() => Test1_GetAllOffersInRadius(10000, 51.380155, 12.493470), 10);
//RepeatTest(() => Test1_GetAllOffersInRadius(50000, 51.380155, 12.493470), 10);
//RepeatTest(() => Test1_GetAllOffersInRadius(100000, 51.380155, 12.493470), 10);
//RepeatTest(() => Test2_GetClosestsOffers(100, 51.380155, 12.493470), 10);
//RepeatTest(() => Test5_GetAllOffersInRadiusWithLevel3Category(10000, 37, 51.380155, 12.493470), 10);
//RepeatTest(() => Test5_GetAllOffersInRadiusWithLevel3Category(50000, 37, 51.380155, 12.493470), 10);
//RepeatTest(() => Test5_GetAllOffersInRadiusWithLevel3Category(100000, 37, 51.380155, 12.493470), 10);
//RepeatTest(() => Test2_GetClosestsOffers(100, 51.380155, 12.493470), 10);
//RepeatTest(() => Test2_GetClosestsOffers(1000, 51.380155, 12.493470), 10);

double Test1_GetAllOffersInRadius(int radiusMeters, double latitude, double longitude)
{
    var sw = Stopwatch.StartNew();

    var center = SqlGeography.Point(latitude, longitude, 4326);
    var mongoIds = connection.Query<byte[]>(
        "SELECT OfferDetailsId FROM Offers WHERE Location.STDistance(@Center) <= @Radius",
        new { Center = center, Radius = radiusMeters }).Select(bytes => new ObjectId(bytes));
    var filter = Builders<Offer>.Filter.In("_id", mongoIds);
    var offers = collection.Find(filter);

    sw.Stop();

    WriteLine($"Get all offers({offers.CountDocuments()}) in {radiusMeters}m radius: {sw.Elapsed.TotalMilliseconds}ms");
    WriteLine($"First offer: {offers.First().Name}");

    return sw.Elapsed.TotalMilliseconds;
}

double Test2_GetClosestsOffers(int number, double latitude, double longitude)
{
    var sw = Stopwatch.StartNew();

    var center = SqlGeography.Point(latitude, longitude, 4326);
    var mongoIds = connection.Query<byte[]>(
        @"SELECT TOP (@Number) OfferDetailsId 
          FROM Offers
          WHERE Location.STDistance(@Center) IS NOT NULL
          ORDER BY Location.STDistance(@Center)",
        new { Number = number, Center = center }).Select(bytes => new ObjectId(bytes));

    var filter = Builders<Offer>.Filter.In("_id", mongoIds);
    var offers = collection.Find(filter);

    sw.Stop();

    WriteLine($"Get {number} closests offers: {sw.Elapsed.TotalMilliseconds}ms");
    WriteLine($"First offer: {offers.First().Name}");

    return sw.Elapsed.TotalMilliseconds;
}

double Test3_GetAllOffersInRadiusWithLevel1Category(int radiusMeters, int level1Category, double latitude, double longitude)
{
    var sw = Stopwatch.StartNew();
 
    var center = SqlGeography.Point(latitude, longitude, 4326);
    var mongoIds = connection.Query<byte[]>(
        "SELECT OfferDetailsId FROM Offers WHERE CategoryLevel1Id = @Category AND Location.STDistance(@Center) <= @Radius",
        new { Category = level1Category, Center = center, Radius = radiusMeters }).Select(bytes => new ObjectId(bytes));
    var filter = Builders<Offer>.Filter.In("_id", mongoIds);
    var offers = collection.Find(filter);
    sw.Stop();

    WriteLine($"Get all offers({offers.CountDocuments()}) in {radiusMeters}m with level 1 category {level1Category}: {sw.Elapsed.TotalMilliseconds}ms");
    WriteLine($"First offer: {offers.First().Name}");

    return sw.Elapsed.TotalMilliseconds;
}

double Test4_GetAllOffersInRadiusWithLevel2Category(int radiusMeters, int level2Category, double latitude, double longitude)
{
    var sw = Stopwatch.StartNew();

    var center = SqlGeography.Point(latitude, longitude, 4326);
    var mongoIds = connection.Query<byte[]>(
        "SELECT OfferDetailsId FROM Offers WHERE CategoryLevel2Id = @Category AND Location.STDistance(@Center) <= @Radius",
        new { Category = level2Category, Center = center, Radius = radiusMeters }).Select(bytes => new ObjectId(bytes));
    var filter = Builders<Offer>.Filter.In("_id", mongoIds);
    var offers = collection.Find(filter);
    sw.Stop();

    WriteLine($"Get all offers({offers.CountDocuments()}) in {radiusMeters}m with level 2 category {level2Category}: {sw.Elapsed.TotalMilliseconds}ms");
    WriteLine($"First offer: {offers.First().Name}");

    return sw.Elapsed.TotalMilliseconds;
}

double Test5_GetAllOffersInRadiusWithLevel3Category(int radiusMeters, int level3Category, double latitude, double longitude)
{
    var sw = Stopwatch.StartNew();

    var center = SqlGeography.Point(latitude, longitude, 4326);
    var mongoIds = connection.Query<byte[]>(
        "SELECT OfferDetailsId FROM Offers WHERE CategoryLevel3Id = @Category AND Location.STDistance(@Center) <= @Radius",
        new { Category = level3Category, Center = center, Radius = radiusMeters }).Select(bytes => new ObjectId(bytes));
    var filter = Builders<Offer>.Filter.In("_id", mongoIds);
    var offers = collection.Find(filter);
    sw.Stop();

    WriteLine($"Get all offers({offers.CountDocuments()}) in {radiusMeters}m with level 3 category {level3Category}: {sw.Elapsed.TotalMilliseconds}ms");
    WriteLine($"First offer: {offers.First().Name}");

    return sw.Elapsed.TotalMilliseconds;
}

void RepeatTest(Func<double> test, int repeats)
{
    double millisecondsSum = 0.0;

    for (int i = 0; i < repeats; i++)
    {
        millisecondsSum += test();
    }

    double avg = millisecondsSum / repeats;

    WriteLine($"Average time over {repeats} repeats: {avg}ms");
    WriteLine();
}

async Task GenerateRandomSqlOffers(int count, string[] words)
{
    const int BufferSize = 100000;

    DataTable table = new DataTable();
    table.Columns.Add("Name", typeof(string));
    table.Columns.Add("Location", typeof(SqlGeography));
    table.Columns.Add("Rating", typeof(int));
    table.Columns.Add("Active", typeof(bool));
    //table.Columns.Add("OfferDetailsId", typeof(byte[]));

    var sb = new StringBuilder();
    for (int i = 0; i < count; i++)
    {
        var offerName = sb.Append(words[random.Next(0, words.Length)])
            .Append(' ')
            .Append(words[random.Next(0, words.Length)])
            .Append(' ')
            .Append(words[random.Next(0, words.Length)])
            .ToString();
        sb.Clear();
        var (latitude, longitude) = GenerateRandomGeography(random, MostWestLong, MostEastLong, MostSouthLat, MostNorthLat);
        table.Rows.Add(
            offerName,                                      // Name
            SqlGeography.Point(latitude, longitude, 4326),  // Location
            random.Next(1, 6),                              // Rating
            true);                                          // Active

        if (table.Rows.Count == BufferSize)
        {
            using var innerBulkCopy = new SqlBulkCopy(connection);
            innerBulkCopy.DestinationTableName = "Offers";
            innerBulkCopy.ColumnMappings.Add("Name", "Name");
            innerBulkCopy.ColumnMappings.Add("Location", "Location");
            innerBulkCopy.ColumnMappings.Add("Rating", "Rating");
            innerBulkCopy.ColumnMappings.Add("Active", "Active");
            //innerBulkCopy.ColumnMappings.Add("OfferDetailsId", "OfferDetailsId");

            try
            {
                innerBulkCopy.WriteToServer(table);
            }
            catch (Exception ex)
            {
                WriteLine(ex.Message);
                throw;
            }

            table.Clear();
        }
    }

    //foreach (var offer in collection.AsQueryable())
    //{
    //    table.Rows.Add(
    //        offer.Name,
    //        SqlGeography.Point(offer.Location.Coordinates.Latitude, offer.Location.Coordinates.Longitude, 4326),
    //        offer.Rating,
    //        offer.Active,
    //        offer.Id.ToByteArray());

    //    if (table.Rows.Count == BufferSize)
    //    {
    //        using var innerBulkCopy = new SqlBulkCopy(connection);
    //        innerBulkCopy.DestinationTableName = "Offers";
    //        innerBulkCopy.ColumnMappings.Add("Name", "Name");
    //        innerBulkCopy.ColumnMappings.Add("Location", "Location");
    //        innerBulkCopy.ColumnMappings.Add("Rating", "Rating");
    //        innerBulkCopy.ColumnMappings.Add("Active", "Active");
    //        innerBulkCopy.ColumnMappings.Add("OfferDetailsId", "OfferDetailsId");

    //        try
    //        {
    //            innerBulkCopy.WriteToServer(table);
    //        }
    //        catch (Exception ex)
    //        {
    //            WriteLine(ex.Message);
    //            throw;
    //        }

    //        table.Clear();
    //    }
    //}

    using var bulkCopy = new SqlBulkCopy(connection);
    bulkCopy.DestinationTableName = "Offers";
    bulkCopy.ColumnMappings.Add("Name", "Name");
    bulkCopy.ColumnMappings.Add("Location", "Location");
    bulkCopy.ColumnMappings.Add("Rating", "Rating");
    bulkCopy.ColumnMappings.Add("Active", "Active");
    //bulkCopy.ColumnMappings.Add("OfferDetailsId", "OfferDetailsId");
    try
    {
        bulkCopy.WriteToServer(table);
    }
    catch (Exception ex)
    {
        WriteLine(ex.Message);
        throw;
    }

    table.Clear();
}

void InsertNewRandomOffers(string[] words, int count, int repeats)
{
    var offers = new List<Offer>(count);

    for (int i = 0; i < count; i++)
    {
        offers.Add(new());
    }

    var sb = new StringBuilder();

    for (int i = 0; i < repeats; i++)
    {
        for (int j = 0; j < count; j++)
        {
            sb.Append(words[random.Next(0, words.Length)])
                .Append(' ')
                .Append(words[random.Next(0, words.Length)])
                .Append(' ')
                .Append(words[random.Next(0, words.Length)]);

            var (latitude, longitude) = GenerateRandomGeography(random, MostWestLong, MostEastLong, MostSouthLat, MostNorthLat);
            offers[j].Id = ObjectId.Empty;
            offers[j].Name = sb.ToString();
            offers[j].Description = faker.Commerce.ProductDescription();
            offers[j].Location = GeoJson.Point(GeoJson.Geographic(longitude, latitude));
            offers[j].Active = true;
            offers[j].Rating = random.Next(1, 5);

            sb.Clear();
        }

        collection.InsertMany(offers);
    }
}

void AssignRandomCategories()
{
    var table = new DataTable();
    table.Columns.Add("OfferId", typeof(int));
    table.Columns.Add("Level1Id", typeof(int));
    table.Columns.Add("Level2Id", typeof(int));
    table.Columns.Add("Level3Id", typeof(int));

    var categoriesIdsSequences = connection.Query<CategoryIdSequence>(
        @"SELECT a.Id AS Level1Id, b.Id AS Level2Id, c.Id AS Level3Id
          FROM CategoriesLevel3 c 
            INNER JOIN CategoriesLevel2 b ON c.CategoryLevel2Id = b.Id
            INNER JOIN CategoriesLevel1 a ON b.CategoryLevel1Id = a.Id").ToList();
    var offersIds = connection.Query<int>("SELECT Id FROM Offers").ToList();

    for (int i = 0; i < offersIds.Count; i++)
    {
        var sequence = categoriesIdsSequences[random.Next(0, categoriesIdsSequences.Count)];

        table.Rows.Add(offersIds[i], sequence.Level1Id, sequence.Level2Id, sequence.Level3Id);
    }

    connection.Execute("CREATE TABLE #TmpTable(OfferId int, Level1Id int, Level2Id int, Level3Id int)");

    using(SqlBulkCopy bulkCopy = new(connection))
    {
        bulkCopy.DestinationTableName = "#TmpTable";
        bulkCopy.ColumnMappings.Add("OfferId", "OfferId");
        bulkCopy.ColumnMappings.Add("Level1Id", "Level1Id");
        bulkCopy.ColumnMappings.Add("Level2Id", "Level2Id");
        bulkCopy.ColumnMappings.Add("Level3Id", "Level3Id");
        bulkCopy.WriteToServer(table);
        bulkCopy.Close();
    }
    
    connection.Execute(@"
        UPDATE Offers
        SET Offers.CategoryLevel1Id = Tmp.Level1Id,
            Offers.CategoryLevel2Id = Tmp.Level2Id,
            Offers.CategoryLevel3Id = Tmp.Level3Id
        FROM Offers INNER JOIN #TmpTable Tmp
            ON Offers.Id = Tmp.OfferId", commandTimeout: 300);
    connection.Execute("DROP TABLE #TmpTable");
}

void AssignKeywords()
{
    DataTable table = new DataTable();
    table.Columns.Add("OfferId", typeof(int));
    table.Columns.Add("KeywordId", typeof(int));

    var keywordsByWord = connection.Query<Keyword>("SELECT * FROM Keywords").ToDictionary(x => x.Word);

    foreach (var offerName in connection.Query<OfferName>("SELECT Id, Name FROM Offers"))
    {
        var offerNameKeywords = offerName.Name.Split(null).Distinct();

        foreach (var offerNameKeyword in offerNameKeywords)
        {
            table.Rows.Add(offerName.Id, keywordsByWord[offerNameKeyword].Id);
        }
    }

    using var innerBulkCopy = new SqlBulkCopy(connection);
    innerBulkCopy.DestinationTableName = "OffersKeywords";
    innerBulkCopy.ColumnMappings.Add("OfferId", "OfferId");
    innerBulkCopy.ColumnMappings.Add("KeywordId", "KeywordId");
    innerBulkCopy.BulkCopyTimeout = 1000;
    innerBulkCopy.WriteToServer(table);
    table.Clear();
}

(double latitude, double longitude) GenerateRandomGeography(Random random, double minLong, double maxLong, double minLat, double maxLat)
{
    var latitude = random.NextDouble() * (maxLat - minLat) + minLat;
    var longitude = random.NextDouble() * (maxLong - minLong) + minLong;

    return (latitude, longitude);
}

void DeleteAllCategories()
{
    connection.Execute("DELETE FROM CategoriesLevel3");
    connection.Execute("DELETE FROM CategoriesLevel2");
    connection.Execute("DELETE FROM CategoriesLevel1");
}

void GenerateCategories()
{
    const int CategoriesLevel1Count = 20;
    const int CategoriesLevel2Count = 10;
    const int CategoriesLevel3Count = 5;

    var categoriesLevel1 = new List<CategoryLevel1>(CategoriesLevel1Count);

    for (int i = 0; i < CategoriesLevel1Count; i++)
    {
        categoriesLevel1.Add(new() { Name = $"Category_{i + 1}", HasSubcategories = true});
    }
    connection.Execute("INSERT INTO CategoriesLevel1(Name, HasSubcategories) VALUES (@Name, @HasSubcategories)", categoriesLevel1);

    foreach (var categoryLevel1 in connection.Query<CategoryLevel1>("SELECT * FROM CategoriesLevel1"))
    {
        var categoriesLevel2 = new List<CategoryLevel2>(CategoriesLevel2Count);
        for (int i = 0; i < CategoriesLevel2Count; i++)
        {
            categoriesLevel2.Add(new() { Name = $"{categoryLevel1.Name}_{i + 1}", HasSubcategories = true, CategoryLevel1Id = categoryLevel1.Id });
        }

        connection.Execute("INSERT INTO CategoriesLevel2(Name, HasSubcategories, CategoryLevel1Id) VALUES (@Name, @HasSubcategories, @CategoryLevel1Id)", categoriesLevel2);
    }

    foreach (var categoryLevel2 in connection.Query<CategoryLevel2>("SELECT * FROM CategoriesLevel2"))
    {
        var categoriesLevel3 = new List<CategoryLevel3>(CategoriesLevel3Count);
        for (int i = 0; i < CategoriesLevel3Count; i++)
        {
            categoriesLevel3.Add(new() { Name = $"{categoryLevel2.Name}_{i + 1}", CategoryLevel2Id = categoryLevel2.Id });
        }

        connection.Execute("INSERT INTO CategoriesLevel3(Name, CategoryLevel2Id) VALUES (@Name, @CategoryLevel2Id)", categoriesLevel3);
    }
}

void InsertKeywords(string[] words, int? append = null)
{
    var keywords = new Keyword[words.Length];
    
    if (append is null)
    {
        for (int i = 0; i < keywords.Length; i++)
        {
            keywords[i] = new() { Word = words[i] };
        }
    }
    else
    {
        for (int i = 0; i < keywords.Length; i++)
        {
            keywords[i] = new() { Word = words[i] + append.ToString() };
        }
    }

    connection.Execute("INSERT INTO Keywords(Word) VALUES(@Word)", keywords);
}

void RemovePopularity(string[] lines)
{
    var newLines = new string[lines.Length];

    for (int i = 0; i < lines.Length; i++)
    {
        newLines[i] = lines[i].Split(null)[0];
    }

    File.WriteAllLines("german-words.txt", newLines);
}

class Offer
{
    public ObjectId Id { get; set; }

    public string Name { get; set; }

    public string Description { get; set; }

    public GeoJsonPoint<GeoJson2DGeographicCoordinates> Location { get; set; }

    public int Rating { get; set; }

    public bool Active { get; set; }
}

class CategoryLevel1
{
    public int Id { get; set; }

    public string Name { get; set; }

    public bool HasSubcategories { get; set; }
}

class CategoryLevel2
{
    public int Id { get; set; }

    public string Name { get; set; }

    public bool HasSubcategories { get; set; }

    public int CategoryLevel1Id { get; set; }
}

class CategoryLevel3
{
    public int Id { get; set; }

    public string Name { get; set; }

    public int CategoryLevel2Id { get; set; }
}

class CategoryIdSequence
{
    public int Level1Id { get; set; }

    public int Level2Id { get; set; }

    public int Level3Id { get; set;}
}

class Keyword
{
    public int Id { get; set; }

    public string Word { get; set; }
}

class OfferName
{
    public int Id  { get; set; }

    public string Name { get; set;}
}
