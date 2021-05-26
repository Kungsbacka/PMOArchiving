param
(
    [parameter(mandatory=$true)]
    [string]
    # Path to PMO export
    $ExportPath,
    # Path to folder for generated metadata
    [parameter(mandatory=$true)]
    [string]
    $MetadataPath,
    # Skip hash (SHA1) validation of files in export (FileList.sha1)
    [switch]
    $SkipFileHashvalidation,
    # Skip validating if all files are present in export and that the files listed
    # in FileList.txt match the files on disk.
    [switch]
    $SkipFileValidation,
    # Handle a special case where a person does not have a personnummer
    [switch]
    $Special
)

# All errors should terminate the script
$ErrorActionPreference = 'Stop'

# These are all the different journal types that exist in the export.
# Each type gets its own metadata file.
$journalTypes = @{
    '1' = @{
        JournalName = 'Skolhälsovårdsjournal'
        FileName = '1_Skolhalsovardsjournal_alla.xml'
    }
    '2' = @{
        JournalName = 'Psykologjournal'
        FileName = '2_Psykologjournal_gr.xml'
    }
    '6' = @{
        JournalName = 'Elevakt'
        FileName = '6_Elevakt_gy.xml'
    }
    '7' = @{
        JournalName = 'Kuratorsakt'
        FileName = '7_Kurators_akt_gy.xml'
    }
    '8' = @{
        JournalName = 'Psykologjournal'
        FileName = '8_Psykologjournal_gy.xml'
    }
    '9' = @{
        JournalName = 'Specialpedagogisk akt'
        FileName = '9_Specialpedagogisk_akt_gy.xml'
    }
    '11' = @{
        JournalName = 'Rektors akt'
        FileName = '11_Rektors_akt_gy.xml'
    }
}

$gymnasieskolor = @(
    'Elof Lindälvs gymnasium'
    'Aranäsgymnasiet'
    'Beda Hallbergs gymnasium'
    'Fristående gymnasieskolor'
    'Gymnasieskola'
    'Externa elever'
)

# Validate paths
$ExportPath, $MetadataPath | ForEach-Object {
    if (-not (Test-Path $_)) {
        Write-Error "Path not found $($_)"
        exit
    }
}

# From this point forward we only work with absolute paths
$ExportPath = (Resolve-Path $ExportPath).ToString()
$MetadataPath = (Resolve-Path $MetadataPath).ToString()

# Find FileList.sha1 files under the export path.
$fileListContents = Get-Content -Path (Join-Path $ExportPath 'FileList.sha1') | Where-Object {$_ -notlike '* Logs\*'}
$index = 1
$JournalFiles = New-Object -TypeName 'System.Collections.ArrayList'
$activity = 'Reading FileList.sha1'
if (-not $SkipFileValidation) {
    $activity += ' and checking that the file exists'
}
if (-not $SkipFileHashvalidation) {
    $activity += ' and hashes match'
}

foreach ($row in $fileListContents)
{
    $expectedHash = $row.Substring(0, 40)
    $relativePath = $row.Substring(41)

    if ($index++ % 30 -eq 0) {
        Write-Progress -Activity $activity -Status $relativePath -PercentComplete ($index / $fileListContents.Count * 100)
    }

    $fullPath = Join-Path $ExportPath $relativePath

    if (-not $SkipFileValidation)
    {
        if (-not (Test-Path $fullPath)) {
            Write-Error "File from FileList.sha1 does not exist on disk: $fullPath"
            exit
        }
    }
    if (-not $SkipFileHashvalidation) {
        $actualHash = (Get-FileHash -Path $fullPath -Algorithm SHA1).Hash
        if ($expectedHash -ne $actualHash)
        {
            Write-Error "Hash do not match. File may be altered or corrupt: $fullPath"
            exit
        }
    }
    $fileName = Split-Path $fullPath -Leaf
    if ($Special)
    {
        # {UUID}_Jrnl1.xml
        $pattern = '{[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}}_Jrnl([\d]+).xml$'
    }
    else
    {
        $pattern = '\d{12}_Jrnl([\d]+).xml$'
    }
    if ($fileName -match $pattern)
    {
        $type = $Matches[1]
        if (-not $journalTypes.ContainsKey($type))
        {
            Write-Error "Unknown journal type: $fullPath"
            exit
        }
        [void]$JournalFiles.Add([pscustomobject]@{
            Path = $fullPath
            JournalType = $type
        })
    }
}
Write-Progress -Activity $activity -Completed

function CreateXmlTextWriter([string]$path)
{
    $w = New-Object -TypeName 'System.Xml.XmlTextWriter' -ArgumentList @($path, [System.Text.Encoding]::UTF8)
    $w.Formatting = 'Indented'
    $w.Indentation = 1
    $w.IndentChar = "`t"
    $w.WriteStartDocument()
    $w.WriteStartElement('pmo')
    $w
}

function DisposeXmlTextWriter([System.Xml.XmlTextWriter]$w)
{
    $w.WriteEndElement() # </pmo>
    $w.WriteEndDocument()
    $w.Dispose()
}

function GenerateAttachmentName([string]$baseName)
{
    $attachmentName = $baseName
    if ($Script:attachmentNameHash.ContainsKey($baseName))
    {
        $attachmentName = $attachmentName + '-' + $Script:attachmentNameHash[$baseName]
        $Script:attachmentNameHash[$baseName]++
    }
    else
    {
        $Script:attachmentNameHash[$baseName] = 1
    }
    $attachmentName
}

$journalGroups = $JournalFiles | Group-Object -Property JournalType
$outer = 1
foreach ($group in $journalGroups)
{
    $journalName = $journalTypes[$group.Name].JournalName
    $metadataFileName = $journalTypes[$group.Name].FileName
    Write-Progress -Activity 'Creating metadata file' -Status $metadataFileName -Id 1 -PercentComplete ($outer++ / $journalGroups.Count * 100)
    $w = CreateXmlTextWriter "$MetadataPath\$metadataFileName"
    $inner = 1
    foreach ($journal in $group.Group)
    {
        Write-Progress -Activity 'Processing journal' -Status $journal.Path -ParentId 1 -PercentComplete ($inner++ / $journalFiles.Count * 100)
        $Script:attachmentNameHash = @{}
        $journalFileName = Split-Path -Path $journal.Path -Leaf
        $journalRelativePath = Split-Path -Path $journal.Path -Parent
        $journalRelativePath = $journalRelativePath.Substring($ExportPath.Length + 1)
        $journalRelativePath = $journalRelativePath.Replace('\', '/')
        $xml = [xml](Get-Content $journal.Path -Encoding UTF8)
        $patient = $xml.JournalExport.PatientData.Patient
        $displayName = $patient.CodeNumber.Substring(2, 6) + '-' + $patient.CodeNumber.Substring(8, 4)
        $w.WriteStartElement('patient')
        $w.WriteElementString('display_name', $displayName)
        $w.WriteElementString('Name', $patient.Name)
        $w.WriteElementString('CodeNumber', $patient.CodeNumber)
        $w.WriteElementString('Sex', $patient.Sex)
        $w.WriteElementString('Address1', $patient.PatientAddress.Address1)
        $w.WriteElementString('ZipCode', $patient.PatientAddress.ZipCode)
        $w.WriteElementString('City', $patient.PatientAddress.City)
        $w.WriteElementString('xmlfil', $journalRelativePath + '/' + $journalFileName)
        $w.WriteStartElement('Document')
        $attachmentName = GenerateAttachmentName $journalName
        $w.WriteElementString('AttachmentName', $attachmentName)
        $w.WriteStartElement('SchoolAttendance')
        $h = New-Object -TypeName 'System.Collections.Generic.HashSet[string]'
        foreach ($school in $patient.SchoolAttendance.School)
        {
            if ($h.Add($school))
            {
                $w.WriteElementString('School', $school)
            }
        }
        $w.WriteEndElement() # </SchoolAttendance>
        $w.WriteStartElement('Attachment')
        $w.WriteStartElement('FileName')
        $w.WriteAttributeString('Type', 'journal')
        $w.WriteString($journalRelativePath + '/' + [System.IO.Path]::GetFileNameWithoutExtension($journalFileName) + '.pdf')
        $w.WriteEndElement() # </FileName>
        $w.WriteEndElement() # </Attachment>
        $w.WriteEndElement() # </Document>
        foreach ($document in $xml.JournalExport.PatientData.Document)
        {
            if ($document.DocAttachment) 
            {
                $w.WriteStartElement('Document')
                $attachmentName = GenerateAttachmentName $document.DocAttachment.AttachmentName
                $w.WriteElementString('AttachmentName', $attachmentName)
                $w.WriteStartElement('Attachment')
                $w.WriteStartElement('FileName')
                $w.WriteAttributeString('Type', 'bilaga')
                $w.WriteString($journalRelativePath + '/' + $document.DocAttachment.Attachment.FileName.Replace('\', '/'))
                $w.WriteEndElement() # </FileName>
                $w.WriteEndElement() # </Attachment>
                $w.WriteEndElement() # </Document>
            }
            else
            {
                Write-Warning "Journal $($journal.Path) contains information about an attachment that does not exist: $($document.DocName)"
            }
        }
        $w.WriteEndElement() # </patient>
    }
    Write-Progress -Activity 'Processing journal' -Completed -ParentId 1
    DisposeXmlTextWriter $w
}
Write-Progress -Activity 'Creating metadata file' -Completed -Id 1

# Split 1_Skolhalsovardsjournal_alla.xml into two files: one with students that has
# attended grundskola only (_gr) and one with students that has attended gymnasieskola
# or both gymnasieskola and grundskola (_gy).
Write-Progress -Activity 'Reading 1_Skolhalsovardsjournal_alla.xml'
$metadataType1 = [xml](Get-Content -Path "$MetadataPath\1_Skolhalsovardsjournal_alla.xml")
Write-Progress -Activity 'Reading 1_Skolhalsovardsjournal_alla.xml' -Completed
$wgr = CreateXmlTextWriter "$MetadataPath\1_Skolhalsovardsjournal_gr.xml"
$wgy = CreateXmlTextWriter "$MetadataPath\1_Skolhalsovardsjournal_gy.xml"
$index = 1
$count = $metadataType1.pmo.patient.Count
foreach ($patient in $metadataType1.pmo.patient)
{
    Write-Progress -Activity 'Processing patient' -Status $patient.CodeNumber -PercentComplete ($index++ / $count * 100)
    if ($patient.Document.SchoolAttendance.School | Where-Object {$_ -in $gymnasieskolor})
    {
        $patient.WriteTo($wgy)
    }
    else
    {
        $patient.WriteTo($wgr)
    }
}
Write-Progress -Activity 'Processing patient' -Completed
DisposeXmlTextWriter $wgr
DisposeXmlTextWriter $wgy
Remove-Item -Path "$MetadataPath\1_Skolhalsovardsjournal_alla.xml" -Confirm:$false
