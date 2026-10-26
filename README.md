# Confluence Extractor

Component ID: `kds-team.ex-confluence`

Downloads pages from Atlassian Confluence: their metadata and content, from all spaces
the account can read. The result is one table, for example to feed a search index,
an AI assistant or documentation analytics.

## Prerequisites

- A Confluence site, for example `https://your-domain.atlassian.net`.
- An account with read access to the spaces you want to download.
- For Confluence Cloud: an [Atlassian API token](https://id.atlassian.com/manage-profile/security/api-tokens)
  for that account.

## Configuration

| Parameter | Required | Description |
|---|---|---|
| Confluence URL (`url`) | yes | Address of your Confluence site. For Confluence Cloud (`*.atlassian.net`), `/wiki` is added automatically when missing. |
| Username (`username`) | yes | The account to log in with. For Confluence Cloud, this is the account's email address. |
| API token (`#api_token`) | yes | API token of that account. Used as the password for HTTP basic authentication. |
| Beautify text (`destination_options.beautify`) | no | Off (default): `text` contains the page body in Confluence storage format (XHTML). On: `text` contains plain text, with the page title and an empty line in front of it. |
| Incremental (`destination_options.incremental`) | no | Off (default): every run downloads all pages and overwrites the table. On: see [Incremental loading](#incremental-loading). |
| Extract attachments (`attachments.enabled`) | no | Off (default). On: the component also writes the table `confluence_attachments`. See [Attachments](#attachments). |
| Download the files (`attachments.download_files`) | no | Off (default). On: the attachment files are also saved to Keboola File Storage. |
| Maximum file size (`attachments.max_file_size_mb`) | no | Default `50`. The component does not download a file larger than this. Fractional values are allowed, for example `0.5`. Use `0` for no limit. |
| Media type filter (`attachments.media_types`) | no | A comma separated list of media type prefixes, for example `image/, application/pdf`. Only matching files are downloaded; the table always holds every attachment. Empty (default) means all files. |

### Sample configuration

```json
{
  "parameters": {
    "url": "https://your-domain.atlassian.net",
    "username": "name@example.com",
    "#api_token": "<API token>",
    "destination_options": {
      "beautify": true,
      "incremental": true
    },
    "attachments": {
      "enabled": true,
      "download_files": true,
      "max_file_size_mb": 50,
      "media_types": ""
    }
  }
}
```

## Output

One table, `confluence_pages`, with primary key `id`:

| Column | Content |
|---|---|
| `id` | `Confluence - <space name>-<page id>` |
| `created_date` | When the page was created |
| `last_updated_date` | When the current version of the page was saved |
| `title` | Page title |
| `creator` | Display name of the author |
| `last_modifier` | Display name of the author of the current version |
| `url` | Link to the page |
| `space` | Space name |
| `text` | Page body; the format depends on **Beautify text** |

Only pages are downloaded. Blog posts and comments are not.
Attachments are downloaded only when you turn them on. See [Attachments](#attachments).

## Attachments

**Extract attachments** is off by default. A configuration that does not set it keeps the behaviour it
has now: one table, `confluence_pages`, and no files.

With **Extract attachments** on, the component writes a second table, `confluence_attachments`, with
primary key `id`. It holds one row per attachment of every page the run extracts:

| Column | Content |
|---|---|
| `id` | Confluence attachment id |
| `page_id` | The `id` of the page in `confluence_pages`. Use it to join the two tables. |
| `space` | Space name |
| `title` | File name in Confluence |
| `media_type` | For example `image/png` or `application/pdf` |
| `file_size` | Size in bytes, as reported by Confluence |
| `created_date` | When the attachment was created |
| `last_updated_date` | When the current version of the attachment was saved |
| `creator` | Display name of the author |
| `download_url` | Direct link to the file in Confluence. It needs the same credentials to open. |
| `file_name` | Name of the file in Keboola File Storage. Empty when the file was not downloaded. |

With **Download the files** also on, the component saves each attachment to Keboola File Storage. The
file name is `<attachment id>_<file name>`, so two pages with a file of the same name do not overwrite
each other. The column `file_name` holds that name.

Each file is stored as **permanent**, so that it is not deleted after the default retention period, and
is tagged with:

| Tag | Purpose |
|---|---|
| `confluence-attachment` | Marks every file this component writes |
| `componentId-<component id>` | The component that wrote the file |
| `configId-<configuration id>` | The configuration that wrote the file |
| `runId-<run id>` | The single run that wrote the file |

Use the `runId` tag to pick the files of one run. Every run uploads its files again, so a configuration
that runs repeatedly keeps one set of files per run.

The component does not download a file larger than **Maximum file size**. It writes a warning to the
log, and the metadata of the file stays in the table with an empty `file_name`.

Use **Media type filter** to download only some attachments, for example only `image/`. The filter
limits the files only: `confluence_attachments` still lists every attachment, so you can see what was
not downloaded.

### Notes

- Attachments are read page by page, which adds one API request per page. A run with attachments on
  takes longer than a run with attachments off.
- The files go to File Storage, not to a table. Download them with the Storage API or a writer, by
  the tag `confluence-attachment`.
- A page the account cannot read the attachments of is logged as a warning and the run continues. The
  page itself is still extracted. The same applies to a single file that fails to download.
- The component downloads a file only from the Confluence host you configured.
- A full backup of a large Confluence site can be many gigabytes. Start with a media type filter or a
  low size limit, check the result, then widen it.

## Incremental loading

With **Incremental** on:

- The component stores the start time of each successful run in its state.
- The next run writes only pages whose current version was saved after that time.
  The first run writes all pages.
- Rows are upserted into `confluence_pages` by `id`.
- Pages deleted in Confluence stay in the table. To remove them, run once with **Incremental** off.
- Attachments are read only for the pages the run writes. An attachment added to a page that did not
  change is therefore not picked up until the page changes. To get all attachments, run once with
  **Incremental** off.

Each run still reads the metadata of every page, so a run with few changes takes about as long as a full run.

## Limitations

- Spaces are read with a single request, so on sites with very many spaces some may be skipped.
  The component logs the name of every space it downloads.

## Development

To run the component locally, put a `config.json` like the sample above into `data/`, then:

```bash
git clone https://github.com/keboola/component-confluence.git
cd component-confluence
docker-compose build
docker-compose run --rm dev
```

Run the tests and the linter:

```bash
docker-compose run --rm test
```

## Integration

For details about deployment and integration with Keboola, see the
[developer documentation](https://developers.keboola.com/extend/component/deployment/).
