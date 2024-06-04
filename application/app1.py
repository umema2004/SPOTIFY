from flask import Flask, render_template, send_from_directory
import re
import string

app = Flask(__name__)

# Function to parse the sample text file
def parse_sample_file(filename):
    records = {}
    with open(filename, 'r') as file:
        current_record = None
        for line in file:
            if line.startswith('Random Records'):
                current_record = int(line.split()[-1].strip(':'))
                records[current_record] = []
            elif line.strip():
                song_info = line.strip().split("'")
                song_tracknum = remove_punctuation(song_info[0])
                song_name = remove_punctuation(song_info[1])
                artist_name = remove_punctuation(song_info[3])
                genre = remove_punctuation(song_info[-1].strip(' []'))
                records[current_record].append((song_name, artist_name, genre, song_tracknum))
    return records

# Function to remove punctuation
def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

# Function to parse the dictionary files
def parse_dict_file(file_name, line_number):
    matching_records = []

    with open(file_name, 'r') as file:
        for line in file:
            if line.startswith(str(line_number)):
                record = line.strip().split(': ')[1]
                matching_records.append(record)

    return matching_records

# Route to display all songs
@app.route('/')
def index():
    records = parse_sample_file('sample.txt')
    return render_template('index.html', records=records)

# Route to display selected song info
@app.route('/song/<int:record_id>/<int:index>')
def song(record_id, index):
    records = parse_sample_file('sample.txt')
    song_name, artist_name, genre, song_tracknum = records[record_id][index]
    record_number = record_id

    line_number = index + 1
    song_tracknum = re.sub(r'[^\d\s]', '', song_tracknum)
    dict_filename = f'dict{record_number}.txt'
    all_records = parse_dict_file(dict_filename, line_number)
    
    return render_template('song.html', song_name=song_name, artist_name=artist_name, genre=genre, record_number=record_number, line_number=line_number, selected_records=all_records, song_tracknum=song_tracknum)

# Route to serve audio files
@app.route('/audio/<path:filename>')
def serve_audio(filename):
    return send_from_directory('static/audio', filename)

if __name__ == '__main__':
    app.run(debug=True)

