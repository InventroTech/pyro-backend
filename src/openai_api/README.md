# OpenAI API Integration

Simple OpenAI file upload and analysis API with no database storage.

## Features

- ✅ **Direct File Upload** - Upload PDF files directly to OpenAI
- ✅ **Custom Prompts** - Ask any question about the uploaded file
- ✅ **No Database** - No local storage, direct processing only
- ✅ **Text Analysis** - Analyze text content without file upload
- ✅ **Clean Response** - Simple JSON responses

## API Endpoints

### 1. File Analysis
**POST** `/openai/analyze-file/`

Upload a PDF file and analyze it with a custom prompt.

**Request:**
```bash
curl -X POST "http://localhost:8001/openai/analyze-file/" \
  -F "file=@/path/to/document.pdf" \
  -F "prompt=What is the first dragon in the book?"
```

**Response:**
```json
{
  "success": true,
  "message": "File analyzed successfully",
  "file_id": "file-abc123xyz",
  "filename": "draconomicon.pdf",
  "prompt": "What is the first dragon in the book?",
  "response": "The first dragon mentioned in the book is the Ancient Red Dragon, described in Chapter 1 as...",
  "processing_time": 3.45
}
```

### 2. Text Analysis
**POST** `/openai/analyze-text/`

Analyze text content directly without file upload.

**Request:**
```bash
curl -X POST "http://localhost:8001/openai/analyze-text/" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Your text content here...",
    "prompt": "Summarize this text in 3 bullet points"
  }'
```

**Response:**
```json
{
  "success": true,
  "message": "Text analyzed successfully",
  "prompt": "Summarize this text in 3 bullet points",
  "response": "• First key point...\n• Second key point...\n• Third key point...",
  "processing_time": 1.23
}
```

## Usage Examples

### Python Example
```python
import requests

# File analysis
with open('document.pdf', 'rb') as file:
    response = requests.post(
        'http://localhost:8001/openai/analyze-file/',
        files={'file': file},
        data={'prompt': 'What are the main topics in this document?'}
    )
    
result = response.json()
if result['success']:
    print(f"Analysis: {result['response']}")
```

### JavaScript Example
```javascript
// File analysis
const formData = new FormData();
formData.append('file', fileInput.files[0]);
formData.append('prompt', 'Analyze this document');

const response = await fetch('/openai/analyze-file/', {
    method: 'POST',
    body: formData
});

const result = await response.json();
if (result.success) {
    console.log('Analysis:', result.response);
}
```

### cURL Examples
```bash
# Analyze a PDF file
curl -X POST "http://localhost:8001/openai/analyze-file/" \
  -F "file=@resume.pdf" \
  -F "prompt=Extract the key skills from this resume"

# Analyze text content
curl -X POST "http://localhost:8001/openai/analyze-text/" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Machine learning is transforming industries...",
    "prompt": "What are the implications of this statement?"
  }'
```

## Setup

1. **Set OpenAI API Key**:
   ```bash
   export PYRO_OPEN_AI_API='your-openai-api-key-here'
   ```

2. **Install OpenAI library** (already in requirements.txt):
   ```bash
   pip install openai
   ```

3. **No migrations needed** - No database tables!

## API Details

### File Upload Validation
- **Supported formats**: PDF only
- **Maximum file size**: 20MB (OpenAI limit)
- **Processing**: Direct upload to OpenAI, no local storage

### OpenAI Integration
- Uses the latest OpenAI Python client
- Supports both standard chat completions and the newer responses API
- Automatic fallback between API methods
- Temporary file cleanup after processing

### Error Handling
- File validation errors
- OpenAI API errors
- Network connectivity issues
- Invalid prompts or responses

## Example Use Cases

1. **Document Analysis**:
   ```
   Prompt: "Summarize the key findings in this research paper"
   ```

2. **Resume Screening**:
   ```
   Prompt: "Extract skills, experience, and education from this resume"
   ```

3. **Contract Review**:
   ```
   Prompt: "What are the main obligations and risks in this contract?"
   ```

4. **Book Analysis**:
   ```
   Prompt: "What is the first dragon mentioned in this book?"
   ```

5. **Report Generation**:
   ```
   Prompt: "Create a 5-point executive summary of this document"
   ```

## No Database Design

This API is intentionally stateless:
- No file storage on server
- No analysis history
- No user accounts required
- Direct processing only
- Clean and simple

Perfect for one-off document analysis tasks!

## Security Notes

- Files are temporarily stored during processing only
- OpenAI files can be automatically deleted (optional)
- No persistent data storage
- PYRO_OPEN_AI_API environment variable required for OpenAI access
- File type and size validation
