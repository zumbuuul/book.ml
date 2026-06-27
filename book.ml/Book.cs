public class Book
{
    public string Name {get;} = "";
    public string Description {get;} = "";
    public string URL {get;} = "";
    public Book()
    {
        
    }
    public Book(string n, string d, string u)
    {
    Name = n;
    Description = d;
    URL = u;
    }
}
