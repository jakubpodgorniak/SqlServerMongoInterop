using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using Bogus;
using Dapper;
using Microsoft.SqlServer.Types;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;

const double MostSouthLong = 47.48667229881244;
const double MostNorthLong = 54.84800488971114;
const double MostWestLat = 6.026129589265816;
const double MostEastLat = 14.540386639801365;

using SqlConnection connection = new ("Server=.\\SQLEXPRESS;Database=ServicePortal4;Integrated Security=true;");
connection.Open();

var client = new MongoClient("mongodb://localhost:27017");
var db =  client.GetDatabase("ServicePortal");
var collection = db.GetCollection<Offer>("Offers");

var faker = new Faker();
var random = new Random();

//GenerateSqlOffersFromNoSql();
//InsertNewRandomOffers(count: 1000, repeats: 100);

async Task GenerateSqlOffersFromNoSql()
{
    Stopwatch sw = Stopwatch.StartNew();

    const int BufferSize = 100000;

    DataTable table = new DataTable();
    table.Columns.Add("Name", typeof(string));
    table.Columns.Add("Location", typeof(SqlGeography));
    table.Columns.Add("Rating", typeof(int));
    table.Columns.Add("Active", typeof(bool));
    table.Columns.Add("OfferDetailsId", typeof(byte[]));

    foreach (var offer in collection.AsQueryable())
    {
        table.Rows.Add(
            offer.Name,
            SqlGeography.Point(offer.Location.Coordinates.Latitude, offer.Location.Coordinates.Longitude, 4326),
            offer.Rating,
            offer.Active,
            offer.Id.ToByteArray());

        if (table.Rows.Count == BufferSize)
        {
            using var innerBulkCopy = new SqlBulkCopy(connection);
            innerBulkCopy.DestinationTableName = "Offers";
            innerBulkCopy.ColumnMappings.Add("Name", "Name");
            innerBulkCopy.ColumnMappings.Add("Location", "Location");
            innerBulkCopy.ColumnMappings.Add("Rating", "Rating");
            innerBulkCopy.ColumnMappings.Add("Active", "Active");
            innerBulkCopy.ColumnMappings.Add("OfferDetailsId", "OfferDetailsId");

            try
            {
                innerBulkCopy.WriteToServer(table);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }

            table.Clear();
        }
    }

    using var bulkCopy = new SqlBulkCopy(connection);
    bulkCopy.DestinationTableName = "Offers";
    bulkCopy.ColumnMappings.Add("Name", "Name");
    bulkCopy.ColumnMappings.Add("Location", "Location");
    bulkCopy.ColumnMappings.Add("Rating", "Rating");
    bulkCopy.ColumnMappings.Add("Active", "Active");
    bulkCopy.ColumnMappings.Add("OfferDetailsId", "OfferDetailsId");
    bulkCopy.WriteToServer(table);

    table.Clear();

    sw.Stop();
}

void InsertNewRandomOffers(int count, int repeats)
{
    var offers = new List<Offer>(count);

    for (int i = 0; i < count; i++)
    {
        offers.Add(new());
    }

    for (int i = 0; i < repeats; i++)
    {
        for (int j = 0; j < count; j++)
        {
            var (longitude, latitude) = GenerateRandomGeography(random, MostSouthLong, MostNorthLong, MostWestLat, MostEastLat);
            offers[j].Id = ObjectId.Empty;
            offers[j].Name = faker.Commerce.ProductName();
            offers[j].Description = faker.Commerce.ProductDescription();
            offers[j].Location = GeoJson.Point(GeoJson.Geographic(longitude, latitude));
            offers[j].Active = true;
            offers[j].Rating = random.Next(1, 5);
        }

        collection.InsertMany(offers);
    }
}

(double longitude, double latitude) GenerateRandomGeography(Random random, double minLong, double maxLong, double minLat, double maxLat)
{
    var longitude = random.NextDouble() * (maxLong - minLong) + minLong;
    var latitude = random.NextDouble() * (maxLat - minLat) + minLat;

    return (longitude, latitude);
}

Console.WriteLine();

class Offer
{
    public ObjectId Id { get; set; }

    public string Name { get; set; }

    public string Description { get; set; }

    public GeoJsonPoint<GeoJson2DGeographicCoordinates> Location { get; set; }

    public int Rating { get; set; }

    public bool Active { get; set; }
}

class SqlOffer
{
    public int Id { get; set; }

    public string Name { get; set; }

    public SqlGeography Location { get; set; }

    public int Rating { get; set; }

    public bool Active { get; set; }

    public byte[] OfferDetailsId { get; set; }
}
