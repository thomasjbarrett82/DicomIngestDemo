using Dapper.Contrib.Extensions;

namespace DicomIngestDemo.Models {
    [Table("IngestHistory")]
    internal class IngestHistory {
        public int Id { get; set; }
        public string Filename { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int ElapsedTimeMilliseconds { get; set; }
    }
}
