from flask import Flask, render_template, request

app = Flask(__name__)

def read_songs(filename):
    sections = {}
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith("Entries for Dictionary Topic"):
                section_number = int(line.split()[-1].strip(":"))
                sections[section_number] = []
            elif line and line[0].isdigit():
                song_info = line.split("[")[1].split("]")[0]
                song_number = int(line.split(":")[0].strip())
                song_name, artist = map(lambda x: x.strip().strip("'"), song_info.split(",")[:2])
                sections[section_number].append((song_number, song_name, artist))
    return sections

def find_section(selected_song_number, sections):
    for section_number, songs in sections.items():
        for song_number, _, _ in songs:
            if song_number == selected_song_number:
                return section_number
    return None

def get_songs_by_section(section_number, sections):
    return sections.get(section_number, [])

def get_all_songs(sections):
    all_songs = []
    for songs in sections.values():
        all_songs.extend(songs)
    return all_songs

@app.route('/')
def index():
    sections = read_songs('sample.txt')
    all_songs = get_all_songs(sections)
    return render_template('index.html', songs=all_songs)

@app.route('/select_song', methods=['POST'])
def select_song():
    selected_song_number = int(request.form['song_number'])
    sections = read_songs('dict.txt')
    section_number = find_section(selected_song_number, sections)
    selected_section_songs = get_songs_by_section(section_number, sections)
    return render_template('selected_song.html', songs=selected_section_songs)

if __name__ == '__main__':
    app.run(debug=True)
