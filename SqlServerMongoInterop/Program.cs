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

var client = new MongoClient("mongodb://localhost:27017");
var db =  client.GetDatabase("ServicePortal");
var collection = db.GetCollection<Offer>("Offers");

var faker = new Faker();
var random = new Random();

GenerateSqlOffersFromNoSql();

async Task GenerateSqlOffersFromNoSql()
{
    Stopwatch sw = Stopwatch.StartNew();

    const int BufferSize = 100000;

    SqlOffer[] sqlOffer = new SqlOffer[BufferSize];
    for (int i = 0; i < sqlOffer.Length; i++)
    {
        sqlOffer[i] = new();
    }

    DataTable table = new DataTable();

    foreach (var offer in collection.AsQueryable())
    {
        int sqlOffersCount = 0;

        sqlOffer.Name = offer.Name;
        sqlOffer.Location = SqlGeography.Point(offer.Location.Coordinates.Latitude, offer.Location.Coordinates.Longitude, 4326);
        sqlOffer.Rating = offer.Rating;
        sqlOffer.Active = offer.Active;
        sqlOffer.OfferDetailsId = offer.Id.ToByteArray();

        try
        {

        connection.Execute(@"Insert Offers(Name, Location, Active, Rating, OfferDetailsId) Values(@Name, @Location, @Rating, @Active, @OfferDetailsId)", sqlOffer);
        }
        catch (Exception ex)
        {
            Console.WriteLine();
        }
    }

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
