# GitHub Actions KuCoin Data Collection Deployment

## 🚀 Quick Deployment

### 1. Create GitHub Repository
- Create a new repository on GitHub
- Name it something like: `kucoin-data-collector`

### 2. Upload Files
Upload these files to your repository:
- `symbols.json` - List of symbols to process
- `package.json` - Dependencies

### 3. Create GitHub Actions Workflow
Create `.github/workflows/data-collection.yml` with this content:

```yaml
name: KuCoin Data Collection
on:
  workflow_dispatch:
    inputs:
      start_index:
        description: Starting symbol index
        required: true
        default: 0
        type: string
      count:
        description: Number of symbols to process
        required: true
        default: 100
        type: string

jobs:
  collect-data:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-node@v4
      with:
        node-version: 20
    - run: npm install
    - run: npm start
```

### 4. Run the Workflow
- Go to Actions tab in your repository
- Click "KuCoin Data Collection"
- Click "Run workflow"
- Set Start Index: 0, Count: 100

## 📊 Configuration

- **Max Concurrent Downloads**: 20 (massive concurrency)
- **Download Delay**: 25ms (very fast)
- **Date Range**: 2020-01-01 to 2024-01-01

## 🔄 Batch Processing

Run multiple workflow instances:
- Run 1: Start Index: 0, Count: 100
- Run 2: Start Index: 100, Count: 100
- Run 3: Start Index: 200, Count: 100

---

**Total Symbols**: 1880
**Created**: 8/20/2025