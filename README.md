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

Only pages are downloaded. Blog posts, comments and attachments are not.

## Incremental loading

With **Incremental** on:

- The component stores the start time of each successful run in its state.
- The next run writes only pages whose current version was saved after that time.
  The first run writes all pages.
- Rows are upserted into `confluence_pages` by `id`.
- Pages deleted in Confluence stay in the table. To remove them, run once with **Incremental** off.

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
