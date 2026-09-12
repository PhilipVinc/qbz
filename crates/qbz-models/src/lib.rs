//! QBZ Models - Shared types, events, and traits
//!
//! This crate provides the foundation for all QBZ crates:
//! - Type definitions (Track, Album, Artist, etc.)
//! - Event definitions (CoreEvent enum)
//! - Trait definitions (FrontendAdapter)
//! - Playback types (QueueTrack, PlaybackState)
//! - Error types
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      qbz-models (Tier 0)                    │
//! │  Types, Events, Traits - No dependencies on other qbz-*    │
//! └─────────────────────────────────────────────────────────────┘
//!                              ↑
//!     ┌────────────────────────┼────────────────────────┐
//!     │                        │                        │
//! ┌───┴───┐              ┌─────┴─────┐            ┌─────┴─────┐
//! │qbz-audio│            │qbz-qobuz  │            │qbz-player │
//! │ Tier 1 │             │  Tier 1   │            │  Tier 2   │
//! └────────┘             └───────────┘            └───────────┘
//! ```
//!
//! # Usage
//!
//! ```rust
//! use qbz_models::{Track, Album, CoreEvent, FrontendAdapter};
//! ```

pub mod error;
pub mod events;
pub mod lenient;
pub mod mixtape;
pub mod playback;
pub mod purchase_serde;
pub mod source;
pub mod system_capabilities;
pub mod traits;
pub mod types;

// Re-export commonly used types at crate root
pub use error::{QbzError, QbzResult};
pub use events::CoreEvent;
pub use lenient::{parse_items_array, parse_items_lenient};
pub use playback::{PlaybackState, PlaybackStatus, QueueState, QueueTrack, RepeatMode};
pub use source::{plex_thumb_url, ArtworkRef, PlaybackSource, TrackOriginTag};
pub use traits::{FrontendAdapter, LoggingAdapter, NoOpAdapter};
pub use types::{
    probe_streaminfo,
    Album,
    // Award types
    AlbumAward,
    AlbumSuggestResponse,
    AlbumSummary,
    Artist,
    ArtistAlbums,
    ArtistBiography,
    ArtistStoryAuthor,
    ArtistStoryImage,
    ArtistStoryItem,
    // Artist page types
    ArtistStoryResponse,
    AssetOrigin,
    AudioParams,
    AwardMagazine,
    AwardPageContainer,
    AwardPageData,
    AwardPageGenericList,
    // Discover types
    DiscoverAlbum,
    DiscoverAlbumDates,
    DiscoverAlbumImage,
    DiscoverArtist,
    DiscoverAudioInfo,
    DiscoverContainer,
    DiscoverContainers,
    DiscoverData,
    DiscoverPlaylist,
    DiscoverPlaylistImage,
    DiscoverPlaylistsResponse,
    DiscoverResponse,
    ExternalStreamAsset,
    Favorites,
    Genre,
    GenreInfo,
    GenreListContainer,
    GenreListResponse,
    ImageSet,
    Label,
    LabelExploreResponse,
    LabelGetListResponse,
    LabelListPage,
    LabelPageContainer,
    LabelPageData,
    LabelPageGenericList,
    LabelStoryResponse,
    MostPopularItem,
    PageArtistAward,
    PageArtistBiography,
    PageArtistImages,
    PageArtistName,
    PageArtistPhysicalSupport,
    PageArtistPlaylist,
    PageArtistPlaylistImages,
    PageArtistPlaylistOwner,
    PageArtistPlaylists,
    PageArtistPortrait,
    PageArtistRelease,
    PageArtistReleaseArtist,
    PageArtistReleaseContributor,
    PageArtistReleaseGroup,
    PageArtistResponse,
    PageArtistRights,
    PageArtistSimilar,
    PageArtistSimilarItem,
    PageArtistTrack,
    PageArtistTrackAlbum,
    Playlist,
    PlaylistDuplicateResult,
    PlaylistGenre,
    PlaylistOwner,
    PlaylistTag,
    PlaylistTagsResponse,
    PlaylistWithTrackIds,
    // Purchase types
    PurchaseAlbum,
    PurchaseFormatOption,
    PurchaseIdsResponse,
    PurchaseResponse,
    PurchaseTrack,
    Quality,
    QualityLimit,
    RadioResponse,
    RawPlaylistTag,
    ReleasesGridResponse,
    SearchAllResults,
    SearchResults,
    SearchResultsPage,
    SessionStartResponse,
    StreamQualityInfo,
    StreamRestriction,
    StreamUrl,
    Track,
    TrackFileUrl,
    TrackToAnalyse,
    TracksContainer,
    UserSession,
};
