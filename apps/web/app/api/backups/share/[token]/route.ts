import { NextRequest, NextResponse } from 'next/server'

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ token: string }> }
) {
  try {
    const { token } = await params
    const apiUrl = process.env.NEXT_PUBLIC_API_URL

    if (!apiUrl) {
      return NextResponse.json(
        { message: 'API URL not configured' },
        { status: 500 }
      )
    }

    // Proxy the request to the Go API server
    const response = await fetch(`${apiUrl}/api/backups/share/${token}`, {
      method: 'GET',
      headers: {
        // Forward any relevant headers if needed
      },
    })

    if (!response.ok) {
      const errorText = await response.text()
      return NextResponse.json(
        { message: errorText || 'Failed to download backup' },
        { status: response.status }
      )
    }

    // Get the blob from the response
    const blob = await response.blob()
    
    // Get the filename from Content-Disposition header if available
    const contentDisposition = response.headers.get('Content-Disposition')
    let filename = 'backup.sql'
    if (contentDisposition) {
      const filenameMatch = contentDisposition.match(/filename="?(.+?)"?$/i)
      if (filenameMatch) {
        filename = filenameMatch[1]
      }
    }

    // Return the blob with appropriate headers
    return new NextResponse(blob, {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    })
  } catch (error) {
    console.error('Error proxying shareable link:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}

