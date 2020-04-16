namespace RabbitMqKafkaConnector.Extensions
{
    public static class StringExtensions
    {
        public static string TrimPrefix(this string text, string prefix)
        {
            return text switch
            {
                var txt when string.IsNullOrEmpty(txt) => txt,
                var txt when txt.StartsWith(prefix) => text.Substring(prefix.Length),
                _ => text,
            };
        }
    }
}