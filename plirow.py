from PIL import Image, ImageDraw, ImageFont

def draw_text_on_image(input_path, output_path, text, font_path="./D2coding.ttf", font_size=40, fill="white"):
    img = Image.open(input_path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    font = ImageFont.truetype(font_path, font_size)

    # 텍스트 크기 계산 (왼쪽 상단 (0, 0)을 기준으로 한 경계 상자)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    image_width, image_height = img.size

    # 중앙 정렬 위치 계산
    position = ((image_width - text_width) // 2, (image_height - text_height) // 2)

    draw.text(position, text, font=font, fill=fill)
    img.save(output_path)
    print(f"저장 완료: {output_path}")

def main():
    input_image = "test.png"
    output_image = "output.png"
    text_to_draw = "텍스트 내용"
    draw_text_on_image(input_image, output_image, text_to_draw)

if __name__ == "__main__":
    main()
