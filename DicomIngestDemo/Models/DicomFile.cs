using Dapper.Contrib.Extensions;

namespace DicomIngestDemo.Models {
    [Table("DicomFile")]
    internal class DicomFile {
        public int Id { get; set; }
        public string SopClassUid { get; set; }
        public string SopInstanceUid { get; set; }
        public string PatientId { get; set; }
        public string StudyInstanceUid { get; set; }
        public string SeriesInstanceUid { get; set; }
        public string DicomJson { get; set; }
    }
}
