package org.example.daybook.uniffi.types

/** JSON-backed facet payloads used by the Compose UI. */
sealed class WellKnownFacet {
    data class Dmeta(val v1: DmetaPayload) : WellKnownFacet()
    data class RefGeneric(val v1: String) : WellKnownFacet()
    data class LabelGeneric(val v1: String) : WellKnownFacet()
    data class TitleGeneric(val v1: String) : WellKnownFacet()
    data class PathGeneric(val v1: String) : WellKnownFacet()
    data class Pending(val v1: PendingPayload) : WellKnownFacet()
    data class Body(val v1: BodyPayload) : WellKnownFacet()
    data class Note(val v1: NotePayload) : WellKnownFacet()
    data class Blob(val v1: BlobPayload) : WellKnownFacet()
    data class BlobPin(val v1: BlobPinPayload) : WellKnownFacet()
    data class ImageMetadata(val v1: ImageMetadataPayload) : WellKnownFacet()
    data class OcrResult(val v1: OcrResultPayload) : WellKnownFacet()
    data class Embedding(val v1: EmbeddingPayload) : WellKnownFacet()

    typealias DmetaPayload = org.example.daybook.uniffi.types.Dmeta
    typealias PendingPayload = org.example.daybook.uniffi.types.Pending
    typealias BodyPayload = org.example.daybook.uniffi.types.Body
    typealias NotePayload = org.example.daybook.uniffi.types.Note
    typealias BlobPayload = org.example.daybook.uniffi.types.Blob
    typealias BlobPinPayload = org.example.daybook.uniffi.types.BlobPin
    typealias ImageMetadataPayload = org.example.daybook.uniffi.types.ImageMetadata
    typealias OcrResultPayload = org.example.daybook.uniffi.types.OcrResult
    typealias EmbeddingPayload = org.example.daybook.uniffi.types.Embedding
}
