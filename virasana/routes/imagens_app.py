import io

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image, ImageDraw
from ajna_commons.utils import ImgEnhance
from ajna_commons.utils.images import PIL_tobytes, draw_bboxes
from bson import ObjectId
from flask import request, Response
from flask_login import login_required
from gridfs import GridFS
from matplotlib.cm import _gen_cmap_registry
from virasana.views import get_image


def draw_bboxes_pil(pil_img: Image, bboxes: list):
    draw = ImageDraw.Draw(pil_img)
    draw.rectangle((bboxes[1] - 2, bboxes[0] - 2, bboxes[3] + 2, bboxes[2] + 2),
                   outline='#2288EE', width=4)
        # image.draw()
    return pil_img


def configure(app):
    """Configura rotas para tratamento de imagens."""

    @login_required
    @app.route('/imagens_cmap')
    def imagens_cmap():
        db = app.config['mongodb']
        fs = GridFS(db)
        _id = request.args.get('_id')
        if not _id:
            return 'Param _id obrigatório'
        # Se não passar colormap, default é contrastcv2
        colormap = request.args.get('colormap', 'contraste')
        image = get_image(_id, 0, pil=True)
        if not image:
            return 'Sem imagem'
        if colormap == 'original':
            pass
        elif colormap == 'contraste':
            image = ImgEnhance.enhancedcontrast_cv2(image)
        else:
            # Evitar erro ao chamar colormaps
            if colormap in _gen_cmap_registry():
                cm = plt.get_cmap(colormap)
                image = ImgEnhance.enhancedcontrast_cv2(image)
                fig = cm(np.array(image))
                fig = (fig[:, :, 0, :3] * 255).astype(np.uint8)
                image = Image.fromarray(fig)
        #Marcar BBOX do Reefer
        marca_reefer =  request.args.get('marca_reefer', 'False').lower() == 'true'
        if marca_reefer:
            _id = ObjectId(_id)
            grid_data = fs.get(_id)
            preds = grid_data.metadata.get('predictions')
            if preds:
                reefer = preds[0].get('reefer')
                if reefer:
                    reefer_bbox = reefer[0].get('reefer_bbox')
                    if reefer_bbox:
                        image = draw_bboxes_pil(image, reefer_bbox)
        figdata = PIL_tobytes(image)

        return Response(response=figdata, mimetype='image/jpeg')
