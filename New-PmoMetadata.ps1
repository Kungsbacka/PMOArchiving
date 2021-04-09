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

# Find all FileList.sha1 files under the export path.
Write-Progress -Activity 'Finding all FileList.sha1 files' -Status 'Searching...'
$fileListFiles = Get-ChildItem -Path $ExportPath -Filter 'FileList.sha1' -File -Recurse
Write-Progress -Activity 'Finding all FileList.sha1 files' -Completed
$index = 1
$allFiles = New-Object -TypeName 'System.Collections.ArrayList'
$JournalFiles = New-Object -TypeName 'System.Collections.ArrayList'
foreach ($fileList in $fileListFiles)
{   
    Write-Progress -Activity 'Reading file list' -Status $fileList.FullName -PercentComplete ($index++ / $fileListFiles.Count * 100)
    $sourcePath = Split-Path -Path $fileList.FullName
    $fileList = Get-Content $fileList.FullName
    $filesInList = New-Object -TypeName 'System.Collections.ArrayList'
    if (-not $SkipFileValidation)
    {
        $filesOnDisk = Get-ChildItem -Path $sourcePath -Recurse -File |
            Where-Object Name -NotIn @('JournalExport.xsd','PatientLog.xsd','CBMKeywordTextType.xsd','DataOriginType.xsd','EHTExport.xsd','ExportKey.txt','FileList.sha1') |
            ForEach-Object {$_.FullName.Substring($sourcePath.Length + 1)}
    }
    foreach ($item in $fileList) {
        $parts = $item -split ' '
        $fileName = $parts[1].Trim()
        if ($fileName -like 'Logs\*')
        {
            continue
        }
        [void]$filesInList.Add($fileName)
        $path = Join-Path -Path $sourcePath -ChildPath $fileName
        $sha1 = $parts[0].Trim()
        [void]$allFiles.Add([pscustomobject]@{
            Path = $path
            CorrectSha1 = $sha1
        })
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
                Write-Error "Unknown journal type: $path"
                exit
            }
            [void]$JournalFiles.Add([pscustomobject]@{
                Path = $path
                JournalType = $type
            })
        }
    }
    if (-not $SkipFileValidation)
    {
        $diff = Compare-Object -ReferenceObject $filesInList -DifferenceObject $filesOnDisk
        if ($diff.Count -ne 0)
        {
            Write-Error "Files in list and on disk does not match: $sourcePath"
            exit
        }
    }
}
Write-Progress -Activity 'Reading file list' -Completed

if (-not $SkipFileHashValidation)
{
    $index = 1
    foreach ($file in $allFiles)
    {
        Write-Progress -Activity 'Checking file hash' -Status $file.Path -PercentComplete ($index++ / $allFiles.Count * 100)
        $actualSha1 = (Get-FileHash -Path $file.Path -Algorithm SHA1).Hash
        if ($file.CorrectSha1 -ne $actualSha1)
        {
            Write-Error "Hashes do not match. File may be altered or corrupt: $($file.Path)"
            exit
        }
    }
    Write-Progress -Activity 'Checking file hash' -Completed
}

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
                Write-Warning "Journal $($journal.Path) is containing information about an attachment that does not exist"
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
